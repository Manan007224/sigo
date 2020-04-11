package store

import (
	"log"
	"strconv"

	pb "github.com/Manan007224/sigo/pkg/proto"
	"github.com/go-redis/redis"

	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
)

type SortedQueue struct {
	Name   string
	Client *redis.Client
}

func (queue *SortedQueue) Add(job *pb.JobPayload) error {
	payload, err := proto.Marshal(job)
	if err != nil {
		return protoMarshalErr
	}
	_, err = queue.Client.ZAdd(queue.Name, redis.Z{Score: float64(job.EnqueueAt), Member: payload}).Result()
	if err != nil {
		return errors.Wrapf(err, "failed to add job to sorted queue")
	}
	return nil
}

func (queue *SortedQueue) Find(jid string, timestamp int64) (bool, error) {
	values, err := queue.find(timestamp, false)
	if err != nil {
		return false, err
	}
	if len(values) == 0 {
		log.Println("no values with the timestamp found")
		return false, nil
	}
	for _, value := range values {
		var job pb.JobPayload
		if err = proto.Unmarshal([]byte(value), &job); err != nil {
			log.Println("job unmarshal error")
		}
		if job.Jid == jid {
			return true, nil
		}
	}
	return false, nil
}

func (queue *SortedQueue) find(timestamp int64, isBeginning bool) ([]string, error) {
	tm := strconv.FormatInt(timestamp, 10)
	var result []string

	startTime := tm
	if isBeginning {
		startTime = "-inf"
	}

	result, err := queue.Client.ZRangeByScore(queue.Name, redis.ZRangeBy{Min: startTime, Max: tm}).Result()
	if err != nil {
		return result, errors.Wrapf(err, "failed to find jobs with the timestmap: %d", timestamp)
	}
	return result, nil
}

func (queue *SortedQueue) remove(value string, jid string) error {
	_, err := queue.Client.ZRem(queue.Name, value).Result()
	if err != nil {
		return errors.Wrapf(err, "failed to remove the job with id: %s", jid)
	}
	return nil
}

func (queue *SortedQueue) Remove(job *pb.JobPayload, timestamp int64) error {
	payload, err := proto.Marshal(job)
	if err != nil {
		return protoMarshalErr
	}
	if err = queue.remove(string(payload), job.Jid); err != nil {
		return err
	}
	return nil
}

func (queue *SortedQueue) Size() (int64, error) {
	return queue.Client.ZCount(queue.Name, "-inf", "+inf").Result()
}

func (queue *SortedQueue) SizeByKey(key int64) (int64, error) {
	values, err := queue.find(key, false)
	return int64(len(values)), err
}

func (queue *SortedQueue) MoveTo(to *Queue, timestamp int64) error {
	tm := strconv.FormatInt(timestamp, 10)
	values, err := queue.find(timestamp, true)
	if err != nil {
		return err
	}

	var result *redis.IntCmd
	if _, err = queue.Client.TxPipelined(func(pipe redis.Pipeliner) error {
		result = pipe.ZRemRangeByScore(queue.Name, "-inf", tm)
		for _, value := range values {
			pipe.LPush(to.Name, []byte(value))
		}
		return nil
	}); err != nil {
		return err
	}

	count, err := result.Result()
	if err != nil {
		return err
	}

	log.Printf("%d jobs moved from %s --> %s", count, queue.Name, to.Name)

	return nil
}

func (queue *SortedQueue) MoveToSorted(name string, timestamp int64) error {
	tm := strconv.FormatInt(timestamp, 10)
	values, err := queue.find(timestamp, true)
	if err != nil {
		return err
	}

	var result *redis.IntCmd
	if _, err = queue.Client.TxPipelined(func(pipe redis.Pipeliner) error {
		result = pipe.ZRemRangeByScore(queue.Name, "-inf", tm)
		for _, value := range values {
			pipe.ZAdd(name, redis.Z{Score: float64(timestamp), Member: value})
		}
		return nil
	}); err != nil {
		return err
	}

	count, err := result.Result()
	if err != nil {
		return err
	}

	log.Printf("%d jobs moved from %s --> %s", count, queue.Name, name)

	return nil
}
