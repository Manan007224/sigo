package store

import "github.com/go-redis/redis"

type Queue struct {
	Name     string
	Priority int32
	client   *redis.Client
}
