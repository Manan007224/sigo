package scheduler

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"github.com/Manan007224/sigo/pkg/manager"
	pb "github.com/Manan007224/sigo/pkg/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"google.golang.org/grpc/peer"
)

var (
	clientContextClosed  = fmt.Errorf("client context closed")
	serverContextClosed  = fmt.Errorf("server context closed")
	oldestExceptableTime = 16
)

type Scheduler struct {
	mgr                 *manager.Manager
	heartBeatMonitor    *sync.Map
	connectedClients    *map[string]bool
	schedulerCancelFunc context.CancelFunc
	schedulerCtx        context.Context
	jobExecutor         *JobsExecutor
}

func NewScheduler(parentCtx context.Context) (*Scheduler, error) {
	var scheduler *Scheduler
	mgr, err := manager.NewManager([]*pb.QueueConfig{})
	if err != nil {
		return scheduler, err
	}
	scheduler.mgr = mgr
	scheduler.schedulerCtx, scheduler.schedulerCancelFunc = context.WithCancel(parentCtx)
	scheduler.heartBeatMonitor = &sync.Map{}
	scheduler.connectedClients = &map[string]bool{}

	// Initialize the background jobs to be ran periodically.
	jobs := []*Job{
		{
			every:     5,
			task:      scheduler.mgr.ProcessScheduledJobs,
			parentCtx: scheduler.schedulerCtx,
		},
		{
			every:     5,
			task:      scheduler.mgr.ProcessExecutingJobs,
			parentCtx: scheduler.schedulerCtx,
		},
		{
			every:     10,
			task:      scheduler.mgr.ProcessFailedJobs,
			parentCtx: scheduler.schedulerCtx,
		},
		{
			every:     10,
			task:      scheduler.checkConnectedClients,
			parentCtx: scheduler.schedulerCtx,
		},
	}
	scheduler.jobExecutor = NewJobsExecutor(jobs)

	// Run all the background jobs
	scheduler.jobExecutor.Run()

	return scheduler, nil
}

func (sc *Scheduler) getClientAddr(rpcContext context.Context) net.Addr {
	p, _ := peer.FromContext(rpcContext)
	return p.Addr
}

func (sc *Scheduler) checkClientOrServerContextClosed(rpcContext context.Context) error {
	select {
	case <-sc.schedulerCtx.Done():
		return serverContextClosed
	case <-rpcContext.Done():
		return rpcContext.Err()
	default:
		return nil
	}
}

func (sc *Scheduler) Discover(ctx context.Context, clientConfig *pb.ClientConfig) (*empty.Empty, error) {
	(*sc.connectedClients)[sc.getClientAddr(ctx).String()] = true
	sc.mgr.AddQueue(clientConfig.Queues...)
	return &empty.Empty{}, nil
}

func (sc *Scheduler) BroadCast(ctx context.Context, job *pb.JobPayload) (*empty.Empty, error) {
	if err := sc.checkClientOrServerContextClosed(ctx); err != nil {
		return nil, err
	}
	if err := sc.mgr.Push(job); err != nil {
		return nil, err
	} else {
		return &empty.Empty{}, nil
	}
}

func (sc *Scheduler) HeartBeat(ctx context.Context, ping *empty.Empty) (*empty.Empty, error) {
	if err := sc.checkClientOrServerContextClosed(ctx); err != nil {
		return nil, err
	}
	sc.heartBeatMonitor.Store(sc.getClientAddr(ctx).String(), time.Now().Unix())
	return &empty.Empty{}, nil
}

func (sc *Scheduler) Fetch(ctx context.Context, queue *pb.Queue) (*pb.JobPayload, error) {
	if err := sc.checkClientOrServerContextClosed(ctx); err != nil {
		return nil, err
	}
	return sc.mgr.Fetch(queue.Name, sc.getClientAddr(ctx).String())
}

func (sc *Scheduler) Acknowledge(ctx context.Context, job *pb.JobPayload) (*empty.Empty, error) {
	if err := sc.checkClientOrServerContextClosed(ctx); err != nil {
		return nil, err
	}
	sc.mgr.Do(func() error {
		return sc.mgr.Acknowledge(job)
	})
	return &empty.Empty{}, nil
}

func (sc *Scheduler) Fail(ctx context.Context, failJob *pb.FailPayload) (*empty.Empty, error) {
	if err := sc.checkClientOrServerContextClosed(ctx); err != nil {
		return nil, err
	}
	sc.mgr.Do(func() error {
		return sc.mgr.Fail(failJob, sc.getClientAddr(ctx).String())
	})
	return &empty.Empty{}, nil
}

func (sc *Scheduler) Shutdown() {
	sc.schedulerCancelFunc()
	sc.mgr.Shutdown()
	sc.jobExecutor.Shutdown()
}

func (sc *Scheduler) checkConnectedClients() {
	exceptablePingTime := time.Now().Add(-time.Duration(2*oldestExceptableTime) * time.Second).Unix()
	for client := range *(sc.connectedClients) {
		lastPingTime, ok := sc.heartBeatMonitor.Load(client)
		if !ok {
			continue
		}
		if lastPingTime.(int64) < exceptablePingTime {
			// TODO - also delete any ongoing RPC calls made by this client.
			delete(*sc.connectedClients, client)
			sc.heartBeatMonitor.Delete(client)

			log.Printf("[CLIENT] %s disconnected", client)
		}
	}
}
