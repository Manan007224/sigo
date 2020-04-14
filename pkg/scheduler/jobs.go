package scheduler

import (
	"context"
	"log"
	"sync"
	"time"
)

type Job struct {
	every     time.Duration
	task      func(till int64) error
	parentCtx context.Context
}

func (j *Job) execute() {
	ticker := time.NewTicker(j.every * time.Second)
	for range ticker.C {
		select {
		case <-j.parentCtx.Done():
			log.Printf(j.parentCtx.Err().Error())
			return
		default:
		}
		j.task(time.Now().Unix())
	}
}

type JobsExecutor struct {
	jobs []*Job
	wg   *sync.WaitGroup
}

func NewJobsExecutor(jobs []*Job) *JobsExecutor {
	jobsExecutor := &JobsExecutor{}
	jobsExecutor.jobs = append(jobsExecutor.jobs, jobs...)
	jobsExecutor.wg = &sync.WaitGroup{}
	return jobsExecutor
}

func (jb *JobsExecutor) Run() {
	for _, job := range jb.jobs {
		jb.wg.Add(1)
		go func(wg *sync.WaitGroup, job *Job) {
			defer wg.Done()
			job.execute()
		}(jb.wg, job)
	}
}

func (jb *JobsExecutor) Shutdown() {
	jb.wg.Wait()
}
