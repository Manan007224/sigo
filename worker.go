package main

import (
	"github.com/gorilla/websocket"
	"log"
	"time"
)

// Worker ..
type Worker struct {
	sigo                *Sigo
	conn                *websocket.Conn
	host                string
	id                  string
	gid                 string
	jobChan             chan *Job
	doneChan            chan *Worker
	clientWorkerTimeout chan bool
	WorkerDone          chan *Worker
	pending             int
	index               int
	lastHeartBeat       time.Time
}

// Run ..
func (w *Worker) Run() {

	go w.heartbeat_client_worker()

	for {
		select {
		case job := <-w.jobChan:
			log.Println(job)
			// w.executeJob(job)
			w.doneChan <- w
		case <-w.clientWorkerTimeout:
			delete(w.sigo.workers, w.conn)
			w.WorkerDone <- w
		}
	}
}

func (w *Worker) heartbeat_client_worker() {

	go func() {
		latestHeartBeat := time.Now()
		if int(latestHeartBeat.Sub(w.lastHeartBeat))/1000000000 > 60 {
			w.clientWorkerTimeout <- true
		}

		time.Sleep(60 * time.Second)
	}()

	for {
		msgType, bytes, err := w.conn.ReadMessage()
		if err != nil {
			log.Println("read err", err)
			break
		}
		msg := string(bytes[:])
		if msgType != websocket.TextMessage && msg != "ping" {
			log.Println("unrecognized message")
		} else {
			w.lastHeartBeat = time.Now()
			log.Println("received ping")
		}

		err = w.conn.WriteMessage(websocket.TextMessage, []byte("pong"))
		if err != nil {
			log.Println("Write Error: ", err)
			break
		}
	}
}
