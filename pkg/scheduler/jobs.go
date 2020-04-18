package scheduler

import (
	"context"
	"log"
	"sync"
	"time"
)

type processJob func(till int64) error
type reaperJob func()

type Job struct {
	every     time.Duration
	task      interface{}
	parentCtx context.Context
}

func (j *Job) execute() {
	ticker := time.NewTicker(j.every * time.Second)
	for {
		select {
		case <-j.parentCtx.Done():
			ticker.Stop()
			log.Printf(j.parentCtx.Err().Error())
			return
		case <-ticker.C:
			switch j.task.(type) {
			case processJob:
				j.task.(processJob)(time.Now().Unix())
			default:
				j.task.(reaperJob)()
			}
		}
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
