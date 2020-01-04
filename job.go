package main

import (
	"time"
	"log"
)


type Job struct {
	Jid              string
	Name             string
	Args             map[string]interface{}
	Queue			 string
	Retry			 int
	Timestamp		 int64
	Result 			 string
	Retry_at		 string
	Enqueue_at		 string
}

func NewJob(jid, name, queue, retry_at, enqueue_at string, retry int, args map[string]interface{}) *Job {
	return &Job{
		Jid:  	jid,
		Name: 	name,
		Args: 	args,
		Queue:	queue,
		Retry:  retry,
		Timestamp: time.Now().Unix(),
		Result: "unprocessed",
		Retry_at: retry_at,
		Enqueue_at: enqueue_at,
	}
}

func (job Job) is_scheduled_later() bool {
	return job.Enqueue_at != ""
}

func (job *Job) QueueLog(queue string) {
	log.Println("job: %s enqueued in %s queue", job.Jid, queue)
}