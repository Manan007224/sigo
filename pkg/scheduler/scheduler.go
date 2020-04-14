package scheduler

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"time"

	"github.com/Manan007224/sigo/pkg/manager"
	pb "github.com/Manan007224/sigo/pkg/proto"
	"google.golang.org/grpc/peer"
)

var (
	clientContextClosed = fmt.Errorf("client context closed")
	serverContextClosed = fmt.Errorf("server context closed")
)

type Scheduler struct {
	mgr *manager.Manager

	// heartBeatMonitor stores the last ping from each client.
	heartBeatMonitor *sync.Map

	// Information of the connected clients. Just for logging purposes.
	connectedClients *sync.Map

	// contextMonitor is used for store separate contexts for each client and cancel that context
	// so that other RPC's serving the same client could be stopped immediately.
	contextMonitor map[net.Addr]context.Context
	cancelMonitor  map[net.Addr]context.CancelFunc

	// schedulerCancelFunc and schedulerCtx are child context's of the server context so could be either used
	// by the scheduler to cancel RPC execution of any other clients or could be cancelled by the server when
	// it gracefully shuts down.
	schedulerCancelFunc context.CancelFunc
	schedulerCtx        context.Context
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
	scheduler.connectedClients = &sync.Map{}
	return scheduler, nil
}

func (sc *Scheduler) getClientAddr(rpcContext context.Context) net.Addr {
	p, _ := peer.FromContext(rpcContext)
	return p.Addr
}

func (sc *Scheduler) getClientContextDone(rpcContext context.Context) <-chan struct{} {
	return sc.contextMonitor[sc.getClientAddr(rpcContext)].Done()
}

// Close the client context from the server side, so any other RPC's serving the same client could
// be cancelled.
func (sc *Scheduler) closeClientContext(rpcContext context.Context) {
	sc.cancelMonitor[sc.getClientAddr(rpcContext)]()
}

func (sc *Scheduler) setClientContext(rpcContext context.Context) {
	childCtx, childCancel := context.WithCancel(sc.schedulerCtx)
	sc.contextMonitor[sc.getClientAddr(rpcContext)] = childCtx
	sc.cancelMonitor[sc.getClientAddr(rpcContext)] = childCancel
}

func (sc *Scheduler) checkClientOrServerContextClosed(rpcContext context.Context) error {
	select {
	case <-sc.schedulerCtx.Done():
		return serverContextClosed
	case <-sc.getClientContextDone(rpcContext):
		return clientContextClosed
	case <-rpcContext.Done():
		return rpcContext.Err()
	default:
		return nil
	}
}

func (sc *Scheduler) Discover(context context.Context, clientConfig *pb.ClientConfig) (*pb.EmptyReply, error) {
	select {
	case <-sc.schedulerCtx.Done():
		return nil, serverContextClosed
	default:
	}
	sc.connectedClients.Store(clientConfig.Id, clientConfig)
	sc.mgr.AddQueue(clientConfig.Queues...)
	sc.setClientContext(context)
	return &pb.EmptyReply{}, nil
}

func (sc *Scheduler) BroadCast(context context.Context, job *pb.JobPayload) (*pb.EmptyReply, error) {
	if err := sc.checkClientOrServerContextClosed(context); err != nil {
		return &pb.EmptyReply{}, err
	}
	if err := sc.mgr.Push(job); err != nil {
		return &pb.EmptyReply{}, err
	} else {
		return &pb.EmptyReply{}, nil
	}
}

func (sc *Scheduler) HearBeat(stream pb.Scheduler_HeartBeatServer) error {
	for {
		ctx := stream.Context()
		if err := sc.checkClientOrServerContextClosed(ctx); err != nil {
			return err
		}

		_, err := stream.Recv()
		if err != nil {
			// Client explicitly closed the stream so need to return reply.
			if err == io.EOF {
				return stream.SendAndClose(&pb.EmptyReply{})
			}

			// client got shutdown entirely.
			log.Printf("client: %s disconnected", sc.getClientAddr(stream.Context()).String())
			// NOT sure if this is the right thing todo.
			sc.closeClientContext(stream.Context())

			return err
		}
		sc.heartBeatMonitor.Store(sc.getClientAddr(ctx), time.Now().Unix())
	}
}

func (sc *Scheduler) Fetch(context context.Context, queue *pb.Queue) (*pb.JobPayload, error) {
	if err := sc.checkClientOrServerContextClosed(context); err != nil {
		return nil, err
	}
	return sc.mgr.Fetch(queue.Name)
}

func (sc *Scheduler) Acknowledge(context context.Context, job *pb.JobPayload) (*pb.EmptyReply, error) {
	sc.mgr.Do(func() error {
		return sc.mgr.Acknowledge(job)
	})
	return &pb.EmptyReply{}, nil
}

func (sc *Scheduler) Fail(context context.Context, failJob *pb.FailPayload) (*pb.EmptyReply, error) {
	sc.mgr.Do(func() error {
		return sc.mgr.Fail(failJob)
	})
	return &pb.EmptyReply{}, nil
}
