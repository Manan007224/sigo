package scheduler

import (
	"context"
	"io"
	"log"
	"net"
	"sync"
	"time"

	"github.com/Manan007224/sigo/pkg/manager"
	pb "github.com/Manan007224/sigo/pkg/proto"
	"google.golang.org/grpc/peer"
)

const (
	JOBACK       = "1"
	JOBERROR     = "2"
	DISCONNECTED = "3"
	SERVERERROR  = "4"
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

func (sc *Scheduler) Discover(context context.Context, clientConfig *pb.ClientConfig) (*pb.EmptyReply, error) {
	return nil, nil
}

func (sc *Scheduler) BroadCast(context context.Context, job *pb.JobPayload) (*pb.JobPushReply, error) {
	select {
	case <-sc.schedulerCtx.Done():
		return &pb.JobPushReply{Code: SERVERERROR}, nil
	case <-sc.getClientContextDone(context):
		return &pb.JobPushReply{Code: DISCONNECTED}, nil
	default:
		if err := sc.mgr.Push(job); err != nil {
			return &pb.JobPushReply{Code: JOBERROR, Error: err.Error()}, nil
		} else {
			return &pb.JobPushReply{Code: JOBACK}, nil
		}
	}
}

func (sc *Scheduler) HearBeat(stream pb.Scheduler_HeartBeatServer) error {
	for {
		select {
		case <-sc.schedulerCtx.Done():
			return nil
		case <-sc.getClientContextDone(stream.Context()):
			return nil
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
