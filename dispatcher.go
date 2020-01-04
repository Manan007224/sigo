package main

import (
	"log"
	"github.com/Sirupsen/logrus"
)

type Dispatcher struct {
	sigo 		*Sigo
	jobChan 	chan *Job
	doneChan	chan bool 
	logger		*logrus.Logger
}

func NewDispatcher(sigo *Sigo, logger *logrus.Logger) *Dispatcher {
	return &Dispatcher {
		sigo: sigo,
		jobChan: make(chan *Job),
		logger: logger,
	}
}

func (disp *Dispatcher) Start() {
	go func() {
		// choose a random queue based on their priority
		for {
			queue := sigo.queueChooser.Pick().(string)
			job, err := sigo.Dequeue(queue)			

			log.Println("job: %s dispatched", job.Jid)

			if err != nil {
				continue
			}
			disp.jobChan <- job
		}
	}()

	for {
		select {
		case job := <-disp.jobChan:
			worker := sigo.getFreeWorker()
			worker.jobChan <- job
		}
	}
}