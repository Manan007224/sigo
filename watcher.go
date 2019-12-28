package main

// Watcher schedules the job in the schedule Queue. It periodically takes the jobs in the sorted order
// based on their time to execute and transfers them in their respective processing queues

import (
	"time"
)

type Watcher struct {
	sigo *Sigo
	terminateChan chan bool
	jobChan chan *Job
}


func NewWatcher(sigo *Sigo) *Watcher {
	return &Watcher {
		sigo: sigo,
		terminateChan: make(chan bool),
		jobChan: make(chan *Job),
	}
}

func (w *Watcher) Watch() {
	go w.listen_for_incoming_jobs()

	for {
		timestamp := time.Now().Unix()
		jobs, err := w.sigo.ZRemByScore(timestamp)

		if err != nil {
			// TODO (not yet sure what will be a good fallback strategy)
			continue
		}

		// send the jobs to the jobchan to transfer them in another queue
		for i := 0; i < len(jobs); i++ {
			temp, _ := w.sigo.Decode([]byte(jobs[i]))
			w.jobChan <- temp
		}	
	}
}

func (w *Watcher) listen_for_incoming_jobs() {
	for {
		select {
		case job := <-w.jobChan:
			// put the job to the enqueue queue again
			w.sigo.Enqueue(job.Queue, job)
		}
	}
}