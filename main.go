package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"sync/atomic"
	"time"
	"github.com/gorilla/websocket"
	uuid "github.com/google/uuid"
)

// Sigo ..
type Sigo struct {
	workers      map[*websocket.Conn]*Worker
	incomingJobs chan *Job
	concurrency  uint32
}

var (
	upgrader = websocket.Upgrader{
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
		CheckOrigin: func(r *http.Request) bool {
			return true
		},
	}
	sigo = &Sigo{
		workers:      make(map[*websocket.Conn]*Worker),
		incomingJobs: make(chan *Job),
		concurrency:  0,
	}
	done       = make(chan *Worker)
	workerDone = make(chan *Worker)
)

func publish(w http.ResponseWriter, r *http.Request) {

	type incomingJob struct {
		Jid  string
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

	pid, er := uuid.Parse(r.URL.Query().Get("pid"))

	if er != nil {
		fmt.Println("error is parsing uuid")
		return
	}

	atomic.AddUint32(&sigo.concurrency, 1)

	worker := &Worker{
		sigo:                sigo,
		conn:                conn,
		// client worker id
		pid:                 pid,
		jobChan:             make(chan *Job),
		clientWorkerTimeout: make(chan bool),
		doneChan:            done,
		WorkerDone:          workerDone,
		index:               int(sigo.concurrency),
		pending:             0,
		lastHeartBeat:       time.Now(),
	}

	// 1 on 1 mapping
	go worker.Run()

	// TODO: Add worker to the pool (array of worker)
}

func ping(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "route %s doesn't exist", r.URL.Path[1:])
}

func main() {
	go Dispatch(sigo.incomingJobs, done, workerDone)

	// handle not found
	http.HandleFunc("/", ping)

	// sigo handles a push-job
	http.HandleFunc("/publish", publish)

	// sigo handles a worker connection
	http.HandleFunc("/consume", consume)

	fmt.Println("server listening on port 3000")

	http.ListenAndServe(":3000", nil)
}
