package main

import (
	"context"
	"embed"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	pb "distributed-pow/part3/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

//go:embed index.html
var webFiles embed.FS

var jobCounter uint64

type WorkerInfo struct {
	ID                string    `json:"worker_id"`
	Address           string    `json:"address"`
	LastHeartbeat     time.Time `json:"last_heartbeat"`
	Alive             bool      `json:"alive"`
	CurrentAssignment string    `json:"current_assignment"`

	assignmentStart  int64
	assignmentEnd    int64
	assignmentToken  uint64
	assignmentCancel context.CancelFunc
}

type JobEvent struct {
	Name string
	Data []byte
}

type NonceRange struct {
	Start int64
	End   int64
}

type WorkerTask struct {
	WorkerID   string
	Address    string
	JobID      string
	Data       string
	Difficulty int
	StartNonce int64
	EndNonce   int64
	Token      uint64
	Context    context.Context
	Cancel     context.CancelFunc
}

type Job struct {
	ID             string    `json:"job_id"`
	Data           string    `json:"data"`
	Difficulty     int       `json:"difficulty"`
	Status         string    `json:"status"`
	NoncesChecked  int64     `json:"nonces_checked"`
	StartTime      time.Time `json:"start_time"`
	ElapsedSeconds float64   `json:"elapsed_seconds"`
	Nonce          int64     `json:"nonce,omitempty"`
	Hash           string    `json:"hash,omitempty"`

	NextNonce     int64                      `json:"-"`
	PendingRanges []NonceRange               `json:"-"`
	Ctx           context.Context            `json:"-"`
	Cancel        context.CancelFunc         `json:"-"`
	Subscribers   map[chan JobEvent]struct{} `json:"-"`
}

type Coordinator struct {
	pb.UnimplementedCoordinatorServiceServer

	mu                sync.Mutex
	workers           map[string]*WorkerInfo
	jobs              map[string]*Job
	rangeSize         int64
	assignmentCounter uint64
}

// ----------------------------------------------------
// SSE EVENTS
// ----------------------------------------------------

func (c *Coordinator) publishJobEventLocked(
	job *Job,
	eventName string,
) {
	job.ElapsedSeconds = time.Since(job.StartTime).Seconds()

	data, err := json.Marshal(job)
	if err != nil {
		log.Println("Failed to encode SSE event:", err)
		return
	}

	event := JobEvent{
		Name: eventName,
		Data: data,
	}

	terminal := eventName == "completed" ||
		eventName == "cancelled"

	for subscriber := range job.Subscribers {
		select {
		case subscriber <- event:
		default:
			if terminal {
				select {
				case <-subscriber:
				default:
				}

				select {
				case subscriber <- event:
				default:
				}
			}
		}
	}
}

// ----------------------------------------------------
// WORKER REGISTRATION
// ----------------------------------------------------

func (c *Coordinator) RegisterWorker(
	ctx context.Context,
	req *pb.RegisterWorkerRequest,
) (*pb.RegisterWorkerResponse, error) {

	if req.WorkerId == "" || req.Address == "" {
		return &pb.RegisterWorkerResponse{
			Success: false,
			Message: "Worker ID and address are required",
		}, nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	worker, exists := c.workers[req.WorkerId]

	if exists {
		worker.Address = req.Address
		worker.LastHeartbeat = time.Now()
		worker.Alive = true
	} else {
		c.workers[req.WorkerId] = &WorkerInfo{
			ID:                req.WorkerId,
			Address:           req.Address,
			LastHeartbeat:     time.Now(),
			Alive:             true,
			CurrentAssignment: "",
		}
	}

	log.Printf(
		"Worker registered: ID=%s, Address=%s",
		req.WorkerId,
		req.Address,
	)

	return &pb.RegisterWorkerResponse{
		Success: true,
		Message: "Worker registered successfully",
	}, nil
}

// ----------------------------------------------------
// HEARTBEAT
// ----------------------------------------------------

func (c *Coordinator) Heartbeat(
	ctx context.Context,
	req *pb.HeartbeatRequest,
) (*pb.HeartbeatResponse, error) {

	c.mu.Lock()
	defer c.mu.Unlock()

	worker, exists := c.workers[req.WorkerId]

	if !exists {
		return &pb.HeartbeatResponse{
			Acknowledged: false,
		}, nil
	}

	worker.LastHeartbeat = time.Now()
	worker.Alive = true

	return &pb.HeartbeatResponse{
		Acknowledged: true,
	}, nil
}

// ----------------------------------------------------
// MONITOR WORKERS
// ----------------------------------------------------

func (c *Coordinator) monitorWorkers() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		c.mu.Lock()

		for _, worker := range c.workers {
			if time.Since(worker.LastHeartbeat) > 15*time.Second {
				if worker.Alive {
					log.Printf(
						"Worker %s is now OFFLINE",
						worker.ID,
					)
				}

				worker.Alive = false

				if worker.assignmentCancel != nil {
					worker.assignmentCancel()
				}
			}
		}

		c.mu.Unlock()
	}
}

// ----------------------------------------------------
// GET /workers
// ----------------------------------------------------

func (c *Coordinator) getWorkers(
	w http.ResponseWriter,
	r *http.Request,
) {
	if r.Method != http.MethodGet {
		http.Error(
			w,
			"Method not allowed",
			http.StatusMethodNotAllowed,
		)
		return
	}

	c.mu.Lock()

	workers := make([]WorkerInfo, 0, len(c.workers))

	for _, worker := range c.workers {
		workers = append(workers, *worker)
	}

	c.mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(workers)
}

// ----------------------------------------------------
// POST /jobs
// ----------------------------------------------------

func (c *Coordinator) createJob(
	w http.ResponseWriter,
	r *http.Request,
) {
	var request struct {
		Data       string `json:"data"`
		Difficulty int    `json:"difficulty"`
	}

	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		http.Error(
			w,
			"Invalid JSON",
			http.StatusBadRequest,
		)
		return
	}

	if strings.TrimSpace(request.Data) == "" {
		http.Error(
			w,
			"Data is required",
			http.StatusBadRequest,
		)
		return
	}

	if request.Difficulty < 1 || request.Difficulty > 8 {
		http.Error(
			w,
			"Difficulty must be between 1 and 8",
			http.StatusBadRequest,
		)
		return
	}

	id := fmt.Sprintf(
		"job-%d",
		atomic.AddUint64(&jobCounter, 1),
	)

	jobCtx, cancel := context.WithCancel(
		context.Background(),
	)

	job := &Job{
		ID:             id,
		Data:           request.Data,
		Difficulty:     request.Difficulty,
		Status:         "RUNNING",
		NoncesChecked:  0,
		StartTime:      time.Now(),
		ElapsedSeconds: 0,
		NextNonce:      0,
		PendingRanges:  make([]NonceRange, 0),
		Ctx:            jobCtx,
		Cancel:         cancel,
		Subscribers:    make(map[chan JobEvent]struct{}),
	}

	c.mu.Lock()
	c.jobs[id] = job
	c.publishJobEventLocked(job, "progress")
	c.mu.Unlock()

	log.Printf(
		"New job created: ID=%s, Difficulty=%d",
		id,
		request.Difficulty,
	)

	go c.dispatchSearch(id)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)

	json.NewEncoder(w).Encode(map[string]string{
		"job_id": id,
		"status": "RUNNING",
	})
}

// ----------------------------------------------------
// GET /jobs
// ----------------------------------------------------

func (c *Coordinator) getJobs(
	w http.ResponseWriter,
	r *http.Request,
) {
	if r.Method != http.MethodGet {
		http.Error(
			w,
			"Method not allowed",
			http.StatusMethodNotAllowed,
		)
		return
	}

	c.mu.Lock()

	jobs := make([]Job, 0, len(c.jobs))

	for _, job := range c.jobs {
		jobCopy := *job

		if job.Status == "RUNNING" {
			jobCopy.ElapsedSeconds = time.Since(
				job.StartTime,
			).Seconds()
		}

		jobs = append(jobs, jobCopy)
	}

	c.mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(jobs)
}

// ----------------------------------------------------
// HANDLE /jobs
// ----------------------------------------------------

func (c *Coordinator) handleJobs(
	w http.ResponseWriter,
	r *http.Request,
) {
	switch r.Method {
	case http.MethodPost:
		c.createJob(w, r)

	case http.MethodGet:
		c.getJobs(w, r)

	default:
		http.Error(
			w,
			"Method not allowed",
			http.StatusMethodNotAllowed,
		)
	}
}

// ----------------------------------------------------
// HANDLE /jobs/{id}
// ----------------------------------------------------

func (c *Coordinator) handleJobByID(
	w http.ResponseWriter,
	r *http.Request,
) {
	id := strings.TrimPrefix(r.URL.Path, "/jobs/")

	if strings.HasSuffix(id, "/events") {
		if r.Method != http.MethodGet {
			http.Error(
				w,
				"Method not allowed",
				http.StatusMethodNotAllowed,
			)
			return
		}

		id = strings.TrimSuffix(id, "/events")
		c.handleJobEvents(w, r, id)
		return
	}

	if id == "" || strings.Contains(id, "/") {
		http.Error(
			w,
			"Invalid job ID",
			http.StatusBadRequest,
		)
		return
	}

	switch r.Method {
	case http.MethodGet:
		c.getJob(w, id)

	case http.MethodDelete:
		c.cancelJob(w, id)

	default:
		w.Header().Set("Allow", "GET, DELETE")
		http.Error(
			w,
			"Method not allowed",
			http.StatusMethodNotAllowed,
		)
	}
}

// ----------------------------------------------------
// GET /jobs/{id}
// ----------------------------------------------------

func (c *Coordinator) getJob(
	w http.ResponseWriter,
	id string,
) {
	c.mu.Lock()

	job, exists := c.jobs[id]

	if !exists {
		c.mu.Unlock()
		http.Error(
			w,
			"Job not found",
			http.StatusNotFound,
		)
		return
	}

	jobCopy := *job

	if job.Status == "RUNNING" {
		jobCopy.ElapsedSeconds = time.Since(
			job.StartTime,
		).Seconds()
	}

	c.mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(jobCopy)
}

// ----------------------------------------------------
// DELETE /jobs/{id}
// ----------------------------------------------------

func (c *Coordinator) cancelJob(
	w http.ResponseWriter,
	id string,
) {
	c.mu.Lock()

	job, exists := c.jobs[id]

	if !exists {
		c.mu.Unlock()
		http.Error(
			w,
			"Job not found",
			http.StatusNotFound,
		)
		return
	}

	if job.Status != "RUNNING" {
		currentStatus := job.Status
		c.mu.Unlock()

		http.Error(
			w,
			"Cannot cancel job. Current status: "+currentStatus,
			http.StatusConflict,
		)
		return
	}

	job.Status = "CANCELLED"
	job.ElapsedSeconds = time.Since(job.StartTime).Seconds()

	// Cancels every worker task using the job context.
	job.Cancel()

	c.publishJobEventLocked(job, "cancelled")

	log.Printf("Job %s has been CANCELLED", id)

	response := map[string]interface{}{
		"job_id":  id,
		"status":  job.Status,
		"message": "Job cancelled successfully",
	}

	c.mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// ----------------------------------------------------
// SSE
// ----------------------------------------------------

func writeSSE(
	w http.ResponseWriter,
	event JobEvent,
) error {
	_, err := fmt.Fprintf(
		w,
		"event: %s\ndata: %s\n\n",
		event.Name,
		event.Data,
	)

	return err
}

func (c *Coordinator) handleJobEvents(
	w http.ResponseWriter,
	r *http.Request,
	id string,
) {
	flusher, ok := w.(http.Flusher)

	if !ok {
		http.Error(
			w,
			"Streaming is not supported",
			http.StatusInternalServerError,
		)
		return
	}

	subscriber := make(chan JobEvent, 10)

	c.mu.Lock()

	job, exists := c.jobs[id]

	if !exists {
		c.mu.Unlock()
		http.Error(
			w,
			"Job not found",
			http.StatusNotFound,
		)
		return
	}

	initialData, err := json.Marshal(job)

	if err != nil {
		c.mu.Unlock()
		http.Error(
			w,
			"Could not encode job",
			http.StatusInternalServerError,
		)
		return
	}

	initialEvent := JobEvent{
		Name: "progress",
		Data: initialData,
	}

	terminal := job.Status == "FOUND" ||
		job.Status == "CANCELLED"

	if terminal {
		initialEvent.Name = strings.ToLower(job.Status)
	} else {
		job.Subscribers[subscriber] = struct{}{}
	}

	c.mu.Unlock()

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")

	if err := writeSSE(w, initialEvent); err != nil {
		if !terminal {
			c.removeSubscriber(id, subscriber)
		}
		return
	}

	flusher.Flush()

	if terminal {
		return
	}

	heartbeat := time.NewTicker(15 * time.Second)
	defer heartbeat.Stop()
	defer c.removeSubscriber(id, subscriber)

	for {
		select {
		case event := <-subscriber:
			if err := writeSSE(w, event); err != nil {
				return
			}

			flusher.Flush()

			if event.Name == "completed" ||
				event.Name == "cancelled" {
				return
			}

		case <-heartbeat.C:
			if _, err := fmt.Fprint(w, ": heartbeat\n\n"); err != nil {
				return
			}

			flusher.Flush()

		case <-r.Context().Done():
			return
		}
	}
}

func (c *Coordinator) removeSubscriber(
	jobID string,
	subscriber chan JobEvent,
) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if job, exists := c.jobs[jobID]; exists {
		delete(job.Subscribers, subscriber)
	}
}

// ----------------------------------------------------
// CONNECT TO WORKER
// ----------------------------------------------------

func connectToWorker(
	address string,
) (
	pb.WorkerServiceClient,
	*grpc.ClientConn,
	error,
) {
	conn, err := grpc.NewClient(
		address,
		grpc.WithTransportCredentials(
			insecure.NewCredentials(),
		),
	)

	if err != nil {
		return nil, nil, err
	}

	client := pb.NewWorkerServiceClient(conn)

	return client, conn, nil
}

// ----------------------------------------------------
// PARALLEL DISPATCH
// ----------------------------------------------------

func (c *Coordinator) dispatchSearch(jobID string) {
	for {
		c.mu.Lock()

		job, exists := c.jobs[jobID]

		if !exists || job.Status != "RUNNING" {
			c.mu.Unlock()
			return
		}

		var tasks []WorkerTask

		// Assign one range to every available worker.
		for _, worker := range c.workers {
			if !worker.Alive ||
				worker.CurrentAssignment != "" {
				continue
			}

			var nonceRange NonceRange

			// Reassign a failed range before allocating a new one.
			if len(job.PendingRanges) > 0 {
				nonceRange = job.PendingRanges[0]
				job.PendingRanges = job.PendingRanges[1:]
			} else {
				nonceRange = NonceRange{
					Start: job.NextNonce,
					End:   job.NextNonce + c.rangeSize,
				}

				job.NextNonce = nonceRange.End
			}

			c.assignmentCounter++
			token := c.assignmentCounter

			workerCtx, workerCancel := context.WithCancel(job.Ctx)

			worker.CurrentAssignment = jobID
			worker.assignmentStart = nonceRange.Start
			worker.assignmentEnd = nonceRange.End
			worker.assignmentToken = token
			worker.assignmentCancel = workerCancel

			tasks = append(tasks, WorkerTask{
				WorkerID:   worker.ID,
				Address:    worker.Address,
				JobID:      jobID,
				Data:       job.Data,
				Difficulty: job.Difficulty,
				StartNonce: nonceRange.Start,
				EndNonce:   nonceRange.End,
				Token:      token,
				Context:    workerCtx,
				Cancel:     workerCancel,
			})
		}

		c.mu.Unlock()

		// Start all selected workers concurrently.
		for _, task := range tasks {
			log.Printf(
				"Assigning job %s to worker %s: [%d, %d)",
				task.JobID,
				task.WorkerID,
				task.StartNonce,
				task.EndNonce,
			)

			go c.runWorkerSearch(task)
		}

		time.Sleep(250 * time.Millisecond)
	}
}

// ----------------------------------------------------
// RUN ONE WORKER SEARCH
// ----------------------------------------------------

func (c *Coordinator) runWorkerSearch(task WorkerTask) {
	client, conn, err := connectToWorker(task.Address)

	if err != nil {
		log.Printf(
			"Cannot connect to worker %s: %v",
			task.WorkerID,
			err,
		)

		c.finishWorkerAssignment(task, false, true)
		return
	}

	defer conn.Close()

	stream, err := client.Search(
		task.Context,
		&pb.SearchRequest{
			JobId:      task.JobID,
			Data:       task.Data,
			Difficulty: int32(task.Difficulty),
			StartNonce: task.StartNonce,
			EndNonce:   task.EndNonce,
		},
	)

	if err != nil {
		log.Printf(
			"Search request failed for worker %s: %v",
			task.WorkerID,
			err,
		)

		c.finishWorkerAssignment(task, false, true)
		return
	}

	var lastChecked int64

	completed := false
	found := false
	failed := false

	for {
		update, err := stream.Recv()

		if err == io.EOF {
			if !completed && !found {
				failed = true
			}
			break
		}

		if err != nil {
			// An error after the job has finished is normal:
			// another worker may have found the solution,
			// or the user may have cancelled the job.
			c.mu.Lock()

			job, exists := c.jobs[task.JobID]
			jobStillRunning := exists && job.Status == "RUNNING"

			c.mu.Unlock()

			if jobStillRunning {
				log.Printf(
					"Stream error from worker %s: %v",
					task.WorkerID,
					err,
				)
				failed = true
			} else {
				log.Printf(
					"Search stopped for worker %s: %v",
					task.WorkerID,
					err,
				)
			}

			break
		}

		c.mu.Lock()

		job, exists := c.jobs[task.JobID]

		if !exists || job.Status != "RUNNING" {
			c.mu.Unlock()
			break
		}

		// Add only the new checked nonce count.
		if update.CheckedNonces > lastChecked {
			delta := update.CheckedNonces - lastChecked
			job.NoncesChecked += delta
			lastChecked = update.CheckedNonces
		}

		job.ElapsedSeconds = time.Since(
			job.StartTime,
		).Seconds()

		// A valid nonce was found.
		if update.Found {
			job.Status = "FOUND"
			job.Nonce = update.Nonce
			job.Hash = update.Hash

			// Cancel every worker searching this job.
			job.Cancel()

			c.publishJobEventLocked(job, "completed")

			log.Printf(
				"Job %s FOUND! Worker=%s Nonce=%d Hash=%s",
				task.JobID,
				task.WorkerID,
				update.Nonce,
				update.Hash,
			)

			found = true
			c.mu.Unlock()
			break
		}

		c.publishJobEventLocked(job, "progress")

		if update.Completed {
			completed = true
		}

		c.mu.Unlock()

		if completed {
			break
		}
	}

	task.Cancel()

	c.finishWorkerAssignment(
		task,
		completed || found,
		failed,
	)
}

// ----------------------------------------------------
// FINISH ASSIGNMENT AND REQUEUE FAILED RANGE
// ----------------------------------------------------

func (c *Coordinator) finishWorkerAssignment(
	task WorkerTask,
	completed bool,
	failed bool,
) {
	task.Cancel()

	c.mu.Lock()
	defer c.mu.Unlock()

	job, jobExists := c.jobs[task.JobID]
	jobStillRunning := jobExists && job.Status == "RUNNING"

	worker, workerExists := c.workers[task.WorkerID]

	// Release the worker only if this is still its assignment.
	if workerExists &&
		worker.assignmentToken == task.Token {

		worker.CurrentAssignment = ""
		worker.assignmentStart = 0
		worker.assignmentEnd = 0
		worker.assignmentCancel = nil

		// Do not mark a worker offline if the job was
		// cancelled or completed normally.
		if failed && jobStillRunning {
			worker.Alive = false

			log.Printf(
				"Worker %s failed. Marked OFFLINE.",
				task.WorkerID,
			)
		}
	}

	if !jobStillRunning {
		return
	}

	if completed {
		log.Printf(
			"Worker %s completed range [%d, %d)",
			task.WorkerID,
			task.StartNonce,
			task.EndNonce,
		)
		return
	}

	// Return an incomplete range to the pending queue.
	job.PendingRanges = append(
		job.PendingRanges,
		NonceRange{
			Start: task.StartNonce,
			End:   task.EndNonce,
		},
	)

	log.Printf(
		"Range [%d, %d) returned to pending queue. It will be reassigned.",
		task.StartNonce,
		task.EndNonce,
	)
}

// ----------------------------------------------------
// MAIN
// ----------------------------------------------------

func main() {
	grpcPort := flag.String(
		"grpc-port",
		"50051",
		"Coordinator gRPC listen port",
	)
	httpPort := flag.String(
		"http-port",
		"8080",
		"Coordinator REST listen port",
	)
	rangeSize := flag.Int64(
		"range-size",
		1000000,
		"Number of nonces assigned per worker range",
	)
	flag.Parse()

	if *rangeSize <= 0 {
		log.Fatal("Range size must be greater than zero")
	}

	coordinator := &Coordinator{
		workers:   make(map[string]*WorkerInfo),
		jobs:      make(map[string]*Job),
		rangeSize: *rangeSize,
	}

	// Start gRPC server.
	listener, err := net.Listen("tcp", ":"+*grpcPort)

	if err != nil {
		log.Fatal("Failed to listen:", err)
	}

	grpcServer := grpc.NewServer()

	pb.RegisterCoordinatorServiceServer(
		grpcServer,
		coordinator,
	)

	go coordinator.monitorWorkers()

	// REST routes.
	mux := http.NewServeMux()

	mux.Handle("/", http.FileServer(http.FS(webFiles)))
	mux.HandleFunc("/workers", coordinator.getWorkers)
	mux.HandleFunc("/jobs", coordinator.handleJobs)
	mux.HandleFunc("/jobs/", coordinator.handleJobByID)

	// REST server.
	httpServer := &http.Server{
		Addr:    ":" + *httpPort,
		Handler: mux,
	}

	go func() {
		log.Printf(
			"Coordinator REST server is running on port %s",
			*httpPort,
		)

		if err := httpServer.ListenAndServe(); err != nil &&
			err != http.ErrServerClosed {
			log.Println("REST server failed:", err)
		}
	}()

	log.Printf(
		"Coordinator gRPC server is running on port %s",
		*grpcPort,
	)

	if err := grpcServer.Serve(listener); err != nil {
		log.Fatal("Coordinator gRPC server failed:", err)
	}
}
