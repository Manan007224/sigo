package store

import (
	"fmt"
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
	return queue.add(job, job.EnqueueAt, false)
}

func (queue *SortedQueue) AddJob(job *pb.JobPayload, at int64) error {
	return queue.add(job, at, false)
}

func (queue *SortedQueue) AddWorkingJob(job *pb.JobPayload, at int64) error {
	return queue.add(job, at, true)
}

func (queue *SortedQueue) add(job *pb.JobPayload, timestamp int64, isWorkingJob bool) error {
	payload, err := proto.Marshal(job)
	if err != nil {
		return protoMarshalErr
	}

	queueKey := queue.Name
	if isWorkingJob {
		queueKey = queue.getWorkingQueueKey(job.ClientId)
	}

	_, err = queue.Client.ZAdd(queueKey, redis.Z{Score: float64(timestamp), Member: payload}).Result()
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

func (queue *SortedQueue) FindJobById(jid, clientId string, timestamp int64) (*pb.JobPayload, error) {
	tm := strconv.FormatInt(timestamp, 10)

	values, err := queue.Client.ZRangeByScore(queue.getWorkingQueueKey(clientId), redis.ZRangeBy{Min: "-inf", Max: tm}).Result()
	if err != nil {
		return nil, errors.Wrapf(err, "failed to find jobs with the timestmap: %d", timestamp)
	}

	if len(values) == 0 {
		log.Println("no values with the timestamp found")
		return nil, nil
	}
	for _, value := range values {
		var job pb.JobPayload
		if err = proto.Unmarshal([]byte(value), &job); err != nil {
			log.Println("job unmarshal error")
		}
		if job.Jid == jid {
			return &job, nil
		}
	}
	return nil, nil
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

func (queue *SortedQueue) Get(till int64) ([]*pb.JobPayload, error) {
	var result []*pb.JobPayload
	values, err := queue.find(till, true)
	if err != nil {
		return result, err
	}

	for _, val := range values {
		var job pb.JobPayload
		if err = proto.Unmarshal([]byte(val), &job); err != nil {
			continue
		}
		result = append(result, &job)
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

func (queue *SortedQueue) RemoveFetchedJob(job *pb.JobPayload) error {
	payload, err := proto.Marshal(job)
	if err != nil {
		return protoMarshalErr
	}
	_, err = queue.Client.ZRem(queue.getWorkingQueueKey(job.ClientId), payload).Result()
	if err != nil {
		return errors.Wrapf(err, "failed to remove the job with id: %s", job.Jid)
	}
	return nil
}

func (queue *SortedQueue) Remove(job *pb.JobPayload) error {
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
	if queue.Name != "working" {
		return queue.Client.ZCount(queue.Name, "-inf", "+inf").Result()
	}

	var size int64 = 0
	values, err := queue.getWorkingQueues()
	if err != nil {
		return size, errors.Wrap(err, "failed to get list of working queues")
	}

	for _, value := range values {
		qs, err := queue.Client.ZCount(queue.getWorkingQueueKey(value), "-inf", "+inf").Result()
		if err != nil {
			log.Println(err)
			continue
		}

		size += qs
	}

	return size, nil
}

func (queue *SortedQueue) SizeByScore(key int64) (int64, error) {
	values, err := queue.find(key, false)
	return int64(len(values)), err
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

func (queue *SortedQueue) getWorkingQueueKey(clientId string) string {
	return fmt.Sprintf("working|%s", clientId)
}

func (queue *SortedQueue) getWorkingQueues() ([]string, error) {
	return queue.Client.Keys("working|*").Result()
}
