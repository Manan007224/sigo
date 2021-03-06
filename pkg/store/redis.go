package store

import (
	"sync"

	pb "github.com/Manan007224/sigo/pkg/proto"
	"github.com/go-redis/redis"
	"github.com/pkg/errors"
)

type Store struct {
	client    *redis.Client
	Scheduled *SortedQueue
	Working   *SortedQueue
	Retry     *SortedQueue
	Queues    map[string]*Queue
	Cache     *sync.Map
}

func NewStore(queueConfig []*pb.QueueConfig) (*Store, error) {
	// Explore a better way to create a client later
	redisClient := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})

	_, err := redisClient.Ping().Result()
	if err != nil {
		return nil, errors.Wrap(err, "failed to create a redis client")
	}

	queueMap := map[string]*Queue{}
	for _, queue := range queueConfig {
		queueMap[queue.Name] = &Queue{
			Name:     queue.Name,
			Priority: queue.Priority,
			Client:   redisClient,
		}
	}

	return &Store{
		client:    redisClient,
		Scheduled: &SortedQueue{Name: "scheduled", Client: redisClient},
		Working:   &SortedQueue{Name: "working", Client: redisClient},
		Retry:     &SortedQueue{Name: "retry", Client: redisClient},
		Queues:    queueMap,
		Cache:     &sync.Map{},
	}, nil
}

func (s *Store) Flush() {
	s.client.FlushAll()
}

func (s *Store) Close() {
	s.client.Close()
}

func (s *Store) GetClient() *redis.Client {
	return s.client
}
