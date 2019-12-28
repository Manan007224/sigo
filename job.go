package main

import (
	"time"
)


type Job struct {
	Jid              string
	Name             string
	Args             map[string]interface{}
	Queue			 string
	Retry			 int
	Timestamp		 int64
}

func NewJob(jid, name, queue string, retry int, args map[string]interface{}) *Job {
	return &Job{
		Jid:  	jid,
		Name: 	name,
		Args: 	args,
		Queue:	queue,
		Retry:  retry,
		Timestamp: time.Now().Unix()
	}
}