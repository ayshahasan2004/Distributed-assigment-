package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"log"
	"net"
	"strings"
	"time"

	pb "distributed-pow/part3/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type WorkerServer struct {
	pb.UnimplementedWorkerServiceServer
	workerID string
}

// Search searches for a valid nonce in the assigned range.
func (w *WorkerServer) Search(
	req *pb.SearchRequest,
	stream grpc.ServerStreamingServer[pb.SearchUpdate],
) error {

	if req.Difficulty < 1 || req.Difficulty > 8 {
		return fmt.Errorf("invalid difficulty")
	}

	if req.StartNonce < 0 || req.EndNonce <= req.StartNonce {
		return fmt.Errorf("invalid nonce range")
	}

	log.Printf(
		"Worker %s started job %s: range [%d, %d)",
		w.workerID, req.JobId, req.StartNonce, req.EndNonce,
	)

	prefix := strings.Repeat("0", int(req.Difficulty))
	var checked int64

	for nonce := req.StartNonce; nonce < req.EndNonce; nonce++ {

		// Check for cancellation
		if err := stream.Context().Err(); err != nil {
			log.Println("Search cancelled:", err)
			return err
		}

		// Calculate SHA-256
		input := fmt.Sprintf("%s%d", req.Data, nonce)
		hashBytes := sha256.Sum256([]byte(input))
		hash := hex.EncodeToString(hashBytes[:])

		checked++

		// Check the required prefix
		if strings.HasPrefix(hash, prefix) {

			log.Printf("Worker found nonce: %d", nonce)

			return stream.Send(&pb.SearchUpdate{
				WorkerId:      w.workerID,
				CheckedNonces: checked,
				Found:         true,
				Nonce:         nonce,
				Hash:          hash,
				Completed:     true,
			})
		}

		// Send progress every 10,000 attempts
		if checked%10000 == 0 {
			err := stream.Send(&pb.SearchUpdate{
				WorkerId:      w.workerID,
				CheckedNonces: checked,
				Found:         false,
				Completed:     false,
			})

			if err != nil {
				return err
			}
		}
	}

	log.Printf("Worker %s completed its range", w.workerID)

	return stream.Send(&pb.SearchUpdate{
		WorkerId:      w.workerID,
		CheckedNonces: checked,
		Found:         false,
		Completed:     true,
	})
}

// connectToCoordinator registers the worker and starts heartbeats.
func connectToCoordinator(
	workerID string,
	workerAddress string,
	coordinatorAddress string,
) {

	conn, err := grpc.NewClient(
		coordinatorAddress,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		log.Fatal("Could not connect to coordinator:", err)
	}

	client := pb.NewCoordinatorServiceClient(conn)

	// Register worker
	ctx, cancel := context.WithTimeout(
		context.Background(), 5*time.Second,
	)

	response, err := client.RegisterWorker(ctx, &pb.RegisterWorkerRequest{
		WorkerId: workerID,
		Address:  workerAddress,
	})

	cancel()

	if err != nil {
		conn.Close()
		log.Fatal("Worker registration failed:", err)
	}

	if !response.Success {
		conn.Close()
		log.Fatal("Registration rejected:", response.Message)
	}

	log.Println("Worker registered:", response.Message)

	// Send heartbeat every 5 seconds
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		defer conn.Close()

		for range ticker.C {

			ctx, cancel := context.WithTimeout(
				context.Background(), 3*time.Second,
			)

			response, err := client.Heartbeat(ctx, &pb.HeartbeatRequest{
				WorkerId: workerID,
			})

			cancel()

			if err != nil {
				log.Println("Heartbeat failed:", err)
				continue
			}

			if response.Acknowledged {
				log.Println("Heartbeat acknowledged")
			} else {
				log.Println("Coordinator did not recognize this worker")
			}
		}
	}()
}

func main() {

	// Command-line configuration
	workerID := flag.String("id", "worker-1", "Worker ID")
	port := flag.String("port", "50052", "Worker gRPC port")
	address := flag.String("address", "", "Worker advertised address")
	coordinator := flag.String(
		"coordinator",
		"localhost:50051",
		"Coordinator gRPC address",
	)

	flag.Parse()

	// Set advertised address if not provided
	workerAddress := *address
	if workerAddress == "" {
		workerAddress = "localhost:" + *port
	}

	// Start worker gRPC server
	listener, err := net.Listen("tcp", ":"+*port)
	if err != nil {
		log.Fatal("Failed to listen:", err)
	}

	grpcServer := grpc.NewServer()

	worker := &WorkerServer{
		workerID: *workerID,
	}

	pb.RegisterWorkerServiceServer(grpcServer, worker)

	// Start serving before registration
	go func() {
		log.Printf(
			"Worker %s gRPC server is running on port %s",
			*workerID, *port,
		)

		if err := grpcServer.Serve(listener); err != nil {
			log.Fatal("Worker server failed:", err)
		}
	}()

	// Register with coordinator
	connectToCoordinator(
		*workerID,
		workerAddress,
		*coordinator,
	)

	select {}
}
