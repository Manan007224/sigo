package main

import (
	"context"
	"log"
	"net"

	pb "github.com/Manan007224/sigo/pkg/proto"
	"github.com/Manan007224/sigo/pkg/scheduler"
	"google.golang.org/grpc"
)

const (
	port = ":50051"
)

func main() {
	listener, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	srv := grpc.NewServer()
	ctx, cancel := context.WithCancel(context.Background())
	scheduler, err := scheduler.NewScheduler(ctx)
	if err != nil {
		log.Fatalf("faild to create scheduler: %v", err)
	}

	pb.RegisterSchedulerServer(srv, scheduler)
}
