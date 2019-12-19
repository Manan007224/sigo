package main

import (
	"encoding/json"
	"fmt"
	"github.com/gorilla/websocket"
	"io/ioutil"
	"log"
	"net/http"
	"time"
)

// Sigo ..
type Sigo struct {
	workers      map[*websocket.Conn]*Worker
	incomingJobs chan *Job
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
	}
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

	// sigo.incomingJobs <- NewJob(data.Jid, data.Name, data.Args)
}

func consume(w http.ResponseWriter, r *http.Request) {
	fmt.Println("reached here")
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("websocket connection failed")
		http.Error(w, "Couldn't open ws connection", http.StatusBadRequest)
		return
	}

	worker := &Worker{
		sigo:          sigo,
		conn:          conn,
		host:          r.URL.Query().Get("host"),
		jobChan:       make(chan *Job),
		workerTimeout: make(chan bool),
		lastHeartBeat: time.Now(),
		utilization:   100,
	}

	go worker.Run()
}

func ping(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "route %s doesn't exist", r.URL.Path[1:])
}

func main() {
	// handle not found
	http.HandleFunc("/", ping)

	// sigo handles a push-job
	http.HandleFunc("/publish", publish)

	// sigo handles a worker connection
	http.HandleFunc("/consume", consume)

	fmt.Println("server listening on port 3000")

	http.ListenAndServe(":3000", nil)
}
