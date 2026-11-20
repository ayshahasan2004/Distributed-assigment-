package main

import (
	"context"
	"log"
	"net"
	"time"

	pb "distributed-pow/part2/proto"

	"google.golang.org/grpc"
)

// ProgressServer implements our gRPC service
type ProgressServer struct {
	pb.UnimplementedProgressMonitorServer
}

// Unary RPC: Get one status response
func (s *ProgressServer) GetStatus(
	ctx context.Context,
	req *pb.StatusRequest,
) (*pb.StatusResponse, error) {

	log.Println("Received GetStatus request:", req.TaskId)

	return &pb.StatusResponse{
		TaskId:   req.TaskId,
		Progress: 50,
		Status:   "RUNNING",
	}, nil
}

// Server Streaming RPC: Send multiple progress updates
func (s *ProgressServer) WatchProgress(
	req *pb.WatchRequest,
	stream grpc.ServerStreamingServer[pb.ProgressUpdate],
) error {

	log.Println("Started watching task:", req.TaskId)

	for progress := 0; progress <= 100; progress += 20 {

		// Check if the client cancelled the request
		if err := stream.Context().Err(); err != nil {
			log.Println("Client disconnected:", err)
			return err
		}

		update := &pb.ProgressUpdate{
			TaskId:   req.TaskId,
			Progress: int32(progress),
			Message:  "Task is progressing",
		}

		// Send update to the client
		if err := stream.Send(update); err != nil {
			return err
		}

		log.Println("Sent progress:", progress)

		if progress < 100 {
			time.Sleep(1 * time.Second)
		}
	}

	return nil
}

func main() {

	// Listen on port 50051
	listener, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatal("Failed to listen:", err)
	}

	// Create gRPC server
	grpcServer := grpc.NewServer()

	// Register our service
	pb.RegisterProgressMonitorServer(
		grpcServer,
		&ProgressServer{},
	)

	log.Println("gRPC Server is running on port 50051")

	if err := grpcServer.Serve(listener); err != nil {
		log.Fatal("Server failed:", err)
	}
}
