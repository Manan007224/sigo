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
	mgr *manager.Manager

	// heartBeatMonitor stores the last ping from each client.
	heartBeatMonitor *sync.Map

	// Information of the connected clients. Just for logging purposes.
	connectedClients *map[string]bool

	// schedulerCancelFunc and schedulerCtx are child context's of the server context so could be either used
	// by the scheduler to cancel RPC execution of any other clients or could be cancelled by the server when
	// it gracefully shuts down.
	schedulerCancelFunc context.CancelFunc
	schedulerCtx        context.Context

	// There are some jobs like ProcessScheduledJobs or ProcessExecutingJobs that need to ran on a cron schedule.
	// jobExecutor is a custom struct which takes in a struct and runs them periodically, and logs their response.
	// You can initialize a job like :-
	// job : &Job {
	// 	every: 5 // job would run every 5 seconds
	// 	task: ProcessExecutingJobs // function which runs every 5 seconds
	// 	parentCtx: context.Background()
	// }
	jobExecutor *JobsExecutor
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
		{every: 5, task: scheduler.mgr.ProcessScheduledJobs, parentCtx: scheduler.schedulerCtx},
		{every: 5, task: scheduler.mgr.ProcessExecutingJobs, parentCtx: scheduler.schedulerCtx},
		{every: 10, task: scheduler.mgr.ProcessFailedJobs, parentCtx: scheduler.schedulerCtx},
	}
	scheduler.jobExecutor = NewJobsExecutor(jobs)

	// Run all the background jobs
	scheduler.jobExecutor.Run()
	go scheduler.checkConnectedClients()

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

func (sc *Scheduler) Discover(context context.Context, clientConfig *pb.ClientConfig) (*empty.Empty, error) {
	(*sc.connectedClients)[sc.getClientAddr(context).String()] = true
	sc.mgr.AddQueue(clientConfig.Queues...)
	return &empty.Empty{}, nil
}

func (sc *Scheduler) BroadCast(context context.Context, job *pb.JobPayload) (*empty.Empty, error) {
	if err := sc.checkClientOrServerContextClosed(context); err != nil {
		return nil, err
	}
	if err := sc.mgr.Push(job); err != nil {
		return nil, err
	} else {
		return &empty.Empty{}, nil
	}
}

func (sc *Scheduler) HeartBeat(context context.Context, ping *empty.Empty) (*empty.Empty, error) {
	if err := sc.checkClientOrServerContextClosed(context); err != nil {
		return nil, err
	}
	sc.heartBeatMonitor.Store(sc.getClientAddr(context).String(), time.Now().Unix())
	return &empty.Empty{}, nil
}

func (sc *Scheduler) Fetch(context context.Context, queue *pb.Queue) (*pb.JobPayload, error) {
	if err := sc.checkClientOrServerContextClosed(context); err != nil {
		return nil, err
	}
	return sc.mgr.Fetch(queue.Name)
}

func (sc *Scheduler) Acknowledge(context context.Context, job *pb.JobPayload) (*empty.Empty, error) {
	if err := sc.checkClientOrServerContextClosed(context); err != nil {
		return nil, err
	}
	sc.mgr.Do(func() error {
		return sc.mgr.Acknowledge(job)
	})
	return &empty.Empty{}, nil
}

func (sc *Scheduler) Fail(context context.Context, failJob *pb.FailPayload) (*empty.Empty, error) {
	if err := sc.checkClientOrServerContextClosed(context); err != nil {
		return nil, err
	}
	sc.mgr.Do(func() error {
		return sc.mgr.Fail(failJob)
	})
	return &empty.Empty{}, nil
}

func (sc *Scheduler) Shutdown() {
	sc.mgr.Shutdown()
	sc.jobExecutor.Shutdown()
}

func (sc *Scheduler) checkConnectedClients() {
	ticker := time.NewTicker(15 * time.Second)
	for {
		select {
		case <-sc.schedulerCtx.Done():
			ticker.Stop()
			return
		case <-ticker.C:
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
	}
}
