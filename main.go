package main

import (
	"net/http"
	"github.com/gorilla/websocket"
	"time"
	"fmt"
	"log"
)

var (
	upgrader = websocket.Upgrader {
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
		CheckOrigin: func(r *http.Request) bool {
			return true
		},
		sigo := &Sigo{
			pool: Pool,
			workers: make(map[*websocket.Conn]*Worker)
		}
	}
)

type Pool []*Worker

type Sigo struct {
	pool Pool
	workers map[*websocket.Conn]*Worker
}

type Worker struct {
	conn *websocket.Conn
	host string
	id string
	gid string // goroutine-id
	jobChan chan *Job
}

func (w *Worker) Run() {
	// listen for the incoming messages
}

type Job struct {
	jid string
	name string
	args []interface{}
}


type Balancer struct {
	Pool 	*Pool
	Done 	chan *Worker
}

func (b *Balancer) Balance(requests <-chan Job) {
	for {
		select {
		case job := <-requests:
			b.dispatch(job)
			fmt.Println(b.Pool)
		case worker := <-b.Done:
			b.complete(worker)
		}
	}
}

func (b *Balancer) dispatch(job *Job) {
	w := heap.Pop(b.Pool).(*Worker)
	w.jobChan <- job
	w.pending += 1
	heap.Push(b.Pool, w)
}

func (b *Balancer) complete(worker *Worker) {
	worker.pending -= 1
	heap.Fix(b.Pool, worker.index)
}

func (p Pool) Len () int { return len(p) }

func (p Pool) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
	p[i].index = i
	p[j].index = j
}

func (p *Pool) Push(w interface{}) {
	worker := w.(*Worker)
	worker.index = p.Len()
	*p = append(*p,  worker)
}

func (p *Pool) Pop() interface{} {
	old := *(p)
	n := len(old)
	item := old[n-1]
	item.index = -1
	*(p) = old[0 : n-1]
	return item
}



func publish(w http.ResponseWriter, r *http.Request) {

}

func subscribe(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("websocket connection failed")
		http.Error(w, "Couldn't open ws connection", http.StatusBadRequest)
		return
	}

	host := r.URL.Query().Get("topic")
	id := r.URL.Query().Get("id")
	gid := r.URL.Query().Get("gid")

	worker := &Worker {
		conn : conn,
		host: host,
		id: id,
		gid: gid,
		jobChan: make(chan *Job),

	}

	go worker.Run()
}

func NewPool() *Pool {
	var p Pool
	heap.Init(&p)
	return &p
}

func init(pool) sigo {
	return &sigo {
		pool: pool,
		workers: make(chan *Worker)
	}
}


func main() {

	pool := NewPool()
	sigo := init(pool)

	jobRequest := make(chan *Job)

	balancer := &Balancer {
		pool: pool,
		done: make(chan Job)
	}

	go balancer.Balance(jobRequest)

	// sigo handles a push-job
	http.HandleFunc("/publish", publish)

	// sigo handles a worker connection
	http.HandleFunc("/consumer", consume)

	fmt.Println("server listening on port 3000")

	http.ListenAndServe(":3000", nil)
}
