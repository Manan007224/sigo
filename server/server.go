package main

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

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
	scheduler, err := scheduler.NewScheduler(ctx, cancel)
	if err != nil {
		log.Fatalf("faild to create scheduler: %v", err)
	}
	pb.RegisterSchedulerServer(srv, scheduler)

	srv.Serve(listener)

	go gracefulShutdown(hookSignals(), cancel, srv, scheduler)

}

func hookSignals() chan os.Signal {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	return sigs
}

func gracefulShutdown(sigs chan os.Signal, cancel context.CancelFunc, srv *grpc.Server, sc *scheduler.Scheduler) {
	<-sigs
	cancel()
	sc.Shutdown()
	srv.GracefulStop()

	log.Printf("finally shut everything")

	os.Exit(1)
}
