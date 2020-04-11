package store

import (
	pb "github.com/Manan007224/sigo/pkg/proto"
	"github.com/pkg/errors"

	"github.com/go-redis/redis"
	"github.com/golang/protobuf/proto"
)

type Queue struct {
	Name     string
	Priority int32
	Client   *redis.Client
}

func (queue *Queue) Add(job *pb.JobPayload) error {
	payload, err := proto.Marshal(job)
	if err != nil {
		return protoMarshalErr
	}
	if _, err = queue.Client.LPush(queue.Name, payload).Result(); err != nil {
		return err
	}
	return nil
}

// TODO - Research whether it's better idea to use Blocking pop.
func (queue *Queue) MoveTo(to *SortedQueue) error {
	if _, err := queue.Client.TxPipelined(func(pipe redis.Pipeliner) error {
		// pop an element from the list
		val, err := queue.Client.RPop(queue.Name).Result()
		if err != nil {
			return errors.Wrapf(err, "failed to pop a job from %s queue", queue.Name)
		}
		var job pb.JobPayload
		if err = proto.Unmarshal([]byte(val), &job); err != nil {
			return protoUnMarshalErr
		}
		// push to job to the sorted queue
		if err = to.Add(&job); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return nil
	}
	return nil
}

func (queue *Queue) Size() int64 {
	count, _ := queue.Client.LLen(queue.Name).Result()
	return count
}
