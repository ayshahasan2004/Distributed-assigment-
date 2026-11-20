package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"time"

	pb "distributed-pow/part2/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {

	// Connect to the gRPC server
	conn, err := grpc.NewClient(
		"localhost:50051",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		log.Fatal("Could not connect:", err)
	}
	defer conn.Close()

	// Create gRPC client
	client := pb.NewProgressMonitorClient(conn)

	// --------------------------------
	// 1. Unary RPC
	// --------------------------------

	fmt.Println("===== Unary RPC =====")

	ctx, cancel := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)

	status, err := client.GetStatus(ctx, &pb.StatusRequest{
		TaskId: "task-1",
	})

	cancel()

	if err != nil {
		log.Fatal("GetStatus failed:", err)
	}

	fmt.Println("Task ID:", status.TaskId)
	fmt.Println("Progress:", status.Progress)
	fmt.Println("Status:", status.Status)

	// --------------------------------
	// 2. Server Streaming RPC
	// --------------------------------

	fmt.Println("\n===== Server Streaming RPC =====")

	streamCtx, streamCancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer streamCancel()

	stream, err := client.WatchProgress(
		streamCtx,
		&pb.WatchRequest{
			TaskId: "task-1",
		},
	)
	if err != nil {
		log.Fatal("WatchProgress failed:", err)
	}

	for {
		update, err := stream.Recv()

		if err == io.EOF {
			fmt.Println("Stream finished!")
			break
		}

		if err != nil {
			log.Fatal("Error receiving update:", err)
		}

		fmt.Printf(
			"Task: %s | Progress: %d%% | Message: %s\n",
			update.TaskId,
			update.Progress,
			update.Message,
		)
	}
}
