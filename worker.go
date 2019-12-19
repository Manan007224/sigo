package main

import (
	"github.com/gorilla/websocket"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"
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
}

// Run ..
func (w *Worker) Run() {

	go w.heartbeat_client_worker()

	for {
		select {
		case job := <-w.jobChan:
			log.Println(job)
		case <-w.workerTimeout:
			w.conn.Close()
			return
		}
	}
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

func (w *Worker) heartbeat_client_worker() {
	for {
		// check for the last hearbeat
		latestHeartBeat := time.Now()
		if int(latestHeartBeat.Sub(w.lastHeartBeat))/1000000000 > 60 {
			w.workerTimeout <- true
		}

		msgType, bytes, err := w.read_message()
		if err != nil {
			log.Println("read err", err)
			break
		}
		msg := string(bytes[:])

		if msgType == websocket.TextMessage {
			w.lastHeartBeat = time.Now()
			w.utilization, _ = strconv.Atoi(strings.Split(msg, "-")[1])

			err = w.write_message(websocket.TextMessage, []byte("ack"))
			if err != nil {
				log.Println("Write Error: ", err)
				break
			}
			time.Sleep(60 * time.Second)
		}
	}
}
