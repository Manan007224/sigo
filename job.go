package main

import (
	"time"
)

// Job ..
type Job struct {
	Jid              string
	Name             string
	Args             []interface{}
	EnqueueTimestamp string
	State            string
}

// NewJob ..
func NewJob(jid, name string, args []interface{}) *Job {
	return &Job{
		Jid:              jid,
		Name:             name,
		Args:             args,
		EnqueueTimestamp: time.Now().String(),
		State:            "unprocessed",
	}
}
