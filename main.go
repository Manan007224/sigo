package main

import (
	"net/http"
	"github.com/gorilla/websocket"
	"fmt"
	"log"
	"encoding/json"
	"io/ioutil"
	"sync/atomic"
)

type Sigo struct {
	workers map[*websocket.Conn]*Worker
	incomingJobs chan *Job
	concurrency uint32
}

type Worker struct {
	conn *websocket.Conn
	host string
	id string
	gid string
	jobChan chan *Job
	pending int
	index int
}

var (
	upgrader = websocket.Upgrader {
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
		CheckOrigin: func(r *http.Request) bool {
			return true
		},
	}
	sigo = &Sigo{
		workers: make(map[*websocket.Conn]*Worker),
		incomingJobs: make(chan *Job),
		concurrency: 0,
	}
)

func publish(w http.ResponseWriter, r *http.Request) {

	type incomingJob struct {
		Jid string
		Name string
		Args []interface{}
	}
	var data incomingJob

	b, err := ioutil.ReadAll(r.Body)
	defer r.Body.Close()

	err = json.Unmarshal(b, &data)
	if err != nil {
		fmt.Println(err)
	}

	sigo.incomingJobs <- NewJob(data.Jid, data.Name, data.Args)
}

func consume(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("websocket connection failed")
		http.Error(w, "Couldn't open ws connection", http.StatusBadRequest)
		return
	}

	host := r.URL.Query().Get("topic")
	id := r.URL.Query().Get("id")
	gid := r.URL.Query().Get("gid")

	atomic.AddUint32(&sigo.concurrency, 1)

	worker := &Worker {
		conn : conn,
		host: host,
		id: id,
		gid: gid,
		jobChan: make(chan *Job),
		index: int(sigo.concurrency),
	}

	go worker.Run()

}

func (w *Worker) Run() {
	for {
		select {
		case job := <-w.jobChan:
			done <-
		}
	}
}

func main() {

	done := make(chan *Worker)
	go Dispatch(sigo.incomingJobs, done)

	// sigo handles a push-job
	http.HandleFunc("/publish", publish)

	// sigo handles a worker connection
	http.HandleFunc("/consume", consume)

	fmt.Println("server listening on port 3000")

	http.ListenAndServe(":3000", nil)
}
