package main

// Watcher schedules the job in the schedule Queue. It periodically takes the jobs in the sorted order
// based on their time to execute and transfers them in their respective processing queues

const DELAYED_QUEUE = "delayed"

type Watcher struct {
	sigo *Sigo
	terminateChan chan bool
	jobChan chan Job
}


func NewWatcher(sigo *Sigo) *Watcher {
	return &Watcher {
		sigo: Sigo,
		terminateChan: make(chan bool),
		jobChan: make(chan Job)
	}
}

func (sigo *Sigo) ZRangeByScore(timestamp int64) ([]string, error) {
	data, err := redis.Strings(conn.Do("ZRANGEBYSCORE", DELAYED_QUEUE, "-inf", timestamp))
	if err != nil {
		return nil, fmt.Errorf("errror in finding all the jobs: %s", err)
	}
	return data, nil
}

func (sigo *Sigo) ZRemByScore(timestamp int64) ([]string, err) {
	jobs, err := sigo.ZRangeByScore(timestamp)
	if err != nil {
		return nil, fmt.Errorf("error in zrangebyscore: %s", err)
	}

	// remove all the jobs
	num, err := conn.Do("ZREMRANGEBYSCORE", DELAYED_QUEUE, "-inf", timestamp)
	if err != nil {
		return nil, fmt.Errorf("error in zremrangebyscore: %s", err)
	}

	return jobs, nil
}	

func (w *Watcher) Watch() {
	go w.listen_for_incoming_jobs()
	for {
		timestamp := time.Now().Unix()
		jobs, err := sigo.ZRemByScore(timestamp)

		if err != nil {
			// TODO (not yet sure what will be a good fallback strategy)
			continue
		}

		// send the jobs to the jobchan to transfer them in another queue
		for job := range jobs {
			w.jobChan <- job
		}	
	}
}