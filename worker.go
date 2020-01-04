package main

import (
	"github.com/gorilla/websocket"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"
	"encoding/json"
	"github.com/Sirupsen/logrus"
)

const(
	POLLING_INTERVAL = 60
	SECONDS = 1000000000
	MAX_UTILIZATION = 100
	PROCESSING_QUEUE = "processing"
)

// Worker ..
type Worker struct {
	sigo          *Sigo
	conn          *websocket.Conn
	jobChan       chan *Job
	workerTimeout chan bool
	lastHeartBeat time.Time
	mtx           sync.Mutex
	utilization   int
	host          string
	logger		  *logrus.Logger
}

func (w *Worker) read_message() (int, []byte, error) {
	w.mtx.Lock()
	defer w.mtx.Unlock()
	return w.conn.ReadMessage()
}

func (w *Worker) write_message(msgType int, msg []byte) error {
	w.mtx.Lock()
	defer w.mtx.Unlock()
	return w.conn.WriteMessage(msgType, msg)
}

func (w *Worker) check_utilization() {
	sigo.mtx.Lock()
	defer sigo.mtx.Unlock()

	if(w.utilization >= MAX_UTILIZATION && sigo.freeWorkers.Contains(w)) {
		sigo.freeWorkers.Remove(w)
		sigo.busyWorkers.Add(w)
	} else if(sigo.busyWorkers.Contains(w)) {
		sigo.freeWorkers.Add(w)
		sigo.busyWorkers.Remove(w)
	}
}

func (w *Worker) Run() {

	go w.read_messages()
	go w.heartbeat_client_worker()

	for {
		select {
		case job := <- w.jobChan:
			b, err := json.Marshal(job)
			if err != nil {
				log.Println("error in marshalling job: %s", job)
			}

			// TODO (add another goroutine in the future to swipe out jobs every 10s)
			err = w.sigo.Enqueue(PROCESSING_QUEUE, job)
			if err != nil {
				continue
			} else {
				job.QueueLog("processing")
			}

			err = w.write_message(websocket.TextMessage, b)
			if err != nil {
				continue
			}
		case <-w.workerTimeout:
			log.Println("worker closing: ", w)
			w.conn.Close()
			return
		}
	}
}

func (w *Worker) read_messages() {
	for {
		_, bytes, err := w.read_message()
		if err != nil {
			log.Println("read err", err)
			break
		}
		msg := string(bytes[:])
		// processing various messages
		switch {
		case strings.Contains(msg, "utilization"):
			w.update_utilization(msg)
		case strings.Contains(msg, "job"):
			w.handle_job_response(msg)
		}
	}
}

func (w *Worker) update_utilization(msg string) {
	w.utilization, _ = strconv.Atoi(strings.Split(msg, "-")[1])
}

func (w *Worker) handle_job_response(msg string) {
	job, _ := w.sigo.Decode([]byte(msg))
	switch {
	case job.Result == "ok":
		log.Println("job: %s successfull", job.Jid)
	case job.Result == "failed":
		log.Println("job: %s failed", job.Jid)
		w.handle_job_failed(job)
	}
}


// push the job to the delayed queue
func (w *Worker) handle_job_failed(job *Job) {
	if job.Retry > 0 {
		job.Retry -= 1
		current_time := time.Now().Add(2 * time.Second)
		key := current_time.Unix()
		err := w.sigo.Zadd("scheduled", key, job)
		if err != nil {
			log.Println("job: %s failed to add to scheduled queue", job.Jid)
		}
	}
}

func (w *Worker) heartbeat_client_worker() {
	for {
		// check for the last hearbeat
		latestHeartBeat := time.Now()
		if int(latestHeartBeat.Sub(w.lastHeartBeat)) / SECONDS > POLLING_INTERVAL {
			w.workerTimeout <- true
		}

		w.lastHeartBeat = time.Now()
		w.check_utilization()

		err := w.write_message(websocket.TextMessage, []byte("ack"))
		if err != nil {
			log.Println("Write Error: ", err)
			break
		}
		time.Sleep(POLLING_INTERVAL * time.Second)
	}
}
