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
	mgr                 *manager.Manager
	heartBeatMonitor    *sync.Map
	contextMonitor      map[net.Addr]context.Context
	cancelMonitor       map[net.Addr]context.CancelFunc
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
	return scheduler, nil
}

func (sc *Scheduler) getClientAddr(rpcContext context.Context) net.Addr {
	p, _ := peer.FromContext(rpcContext)
	return p.Addr
}

func (sc *Scheduler) getClientContextDone(rpcContext context.Context) <-chan struct{} {
	return sc.contextMonitor[sc.getClientAddr(rpcContext)].Done()
}

func (sc *Scheduler) closeClientContext(rpcContext context.Context) {
	sc.cancelMonitor[sc.getClientAddr(rpcContext)]()
}

func (sc *Scheduler) checkClientOrServerContextClosed(rpcContext context.Context) error {
	select {
	case <-sc.schedulerCtx.Done():
		return serverContextClosed
	case <-sc.getClientContextDone(rpcContext):
		return clientContextClosed
	default:
		return nil
	}
}

func (sc *Scheduler) Discover(context context.Context, clientConfig *pb.ClientConfig) (*pb.EmptyReply, error) {
	return nil, nil
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
		if err := sc.checkClientOrServerContextClosed(stream.Context()); err != nil {
			return err
		}

		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		default:
		}

		_, err := stream.Recv()
		if err != nil {
			// client got shutdown entirely.
			log.Printf("client: %s disconnected", sc.getClientAddr(stream.Context()).String())
			sc.closeClientContext(stream.Context())

			// Client explicitly closed the stream so need to return reply.
			if err == io.EOF {
				return stream.SendAndClose(&pb.EmptyReply{})
			}
		}
		sc.heartBeatMonitor.Store(sc.getClientAddr(stream.Context()), time.Now().Unix())
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
