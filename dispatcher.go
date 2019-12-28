package main

type Dispatcher struct {
	sigo 		*Sigo
	jobChan 	chan *Job
	doneChan	chan bool 
}

func NewDispatcher(sigo *Sigo) *Dispatcher {
	return &Dispatcher {
		sigo: sigo,
		jobChan: make(chan *Job),
	}
}

func (disp *Dispatcher) Start() {
	go func() {
		// choose a random queue based on their priority
		for {
			queue := sigo.queueChooser.Pick().(string)
			job, err := sigo.Dequeue(queue)			

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