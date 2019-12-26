package main

import (
	"encoding/json"
	"fmt"
	"github.com/gorilla/websocket"
	"io/ioutil"
	"log"
	"net/http"
	"time"
	"github.com/emirpasic/gods/sets/hashset"
	"github.com/gomodule/redigo/redis"
	wr "github.com/mroth/weightedrand"
	"math/rand"
)

// Sigo ..
type Sigo struct {
	workers 		[]*Worker
	incomingJobs 	chan *Job
	pool		 	*redis.Pool
	jobQueues	 	map[string]int
	queueChooser	wr.Chooser
	freeWorkers		*hashset.Set
	busyWorkers		*hashset.Set
}

var (
	upgrader = websocket.Upgrader {
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
		CheckOrigin: func(r *http.Request) bool {
			return true
		},
	}
	sigo = NewSigo()
)

func NewSigo() *Sigo {
	return &Sigo {
		workers:      []*Worker{},
		incomingJobs: make(chan *Job),
		pool:		  initPool(),
	}
}

func initPool() *redis.Pool {
	return &redis.Pool{
		MaxIdle:     5,
		IdleTimeout: time.Duration(240) * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", "127.0.0.1:6379")
			if err != nil {
				return c, nil
			}
			return c, nil
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}
}

func (sigo *Sigo) Encode(item interface{}) (string, error) {
	b, err := json.Marshal(item)
	if err != nil {
		return "", fmt.Errorf("encode data failed: %s", err)
	}
	return string(b), nil
}

func (sigo *Sigo) Decode(data []byte) (map[string]interface{}, error) {
	var b map[string]interface{}
	if err := json.Unmarshal(data, &b); err != nil {
		return b, fmt.Errorf("decode data failed: %s", err)
	}

	return b, nil
}

func (sigo *Sigo) Enqueue(job *Job) error {
	conn := sigo.pool.Get()
	defer conn.Close()

	queue := job.Queue
	if queue == "" {
		return fmt.Errorf("Job doesn't have a queue")
	}

	value, err := sigo.Encode(job)
	if err != nil {
		return fmt.Errorf("enqueue job failed: %s", err)
	}

	// TODO - failover mechanism
	_, err = conn.Do("RPUSH", queue, value)
	if err != nil {
		fmt.Errorf("enqueue failed for the job: %s", job)
	}

	_, err = conn.Do("SADD", sigo.jobQueues, queue)	
	if err != nil {
		fmt.Errorf("enqueue failed for the job: %s", job)
	}
	return nil
}

// func (sigo *Sigo) AddQueues(queues map[string]int) error {
// 	for k, v := range queues {

// 	}
// }

func (sigo *Sigo) Dequeue(queue string) (*Job, error) {
	conn := sigo.pool.Get()
	defer conn.Close()

	reply, err := conn.Do("LPOP", queue)
	if err != nil {
		return nil, fmt.Errorf("dequeue failed for the queue: %s", queue)
	}
	data, err := redis.Bytes(reply, err)
	if err != nil {
		return nil, fmt.Errorf("pop failed: %s", err)
	}

	var job Job
	err = json.Unmarshal(data, &job)
	if err != nil {
		return nil, fmt.Errorf("error in decoding job")
	}
	return &job, nil
}

func (sigo *Sigo) getFreeWorker() *Worker {
	for {
		workers := sigo.freeWorkers.Values()
		if len(workers) == 0 {
			continue
		} else {
			return workers[rand.Int() % len(workers)].(*Worker)
		}
	}
}

func publish(w http.ResponseWriter, r *http.Request) {
	var data Job

	b, err := ioutil.ReadAll(r.Body)
	defer r.Body.Close()

	err = json.Unmarshal(b, &data)
	if err != nil {
		fmt.Println(err)
	}

	fmt.Println(data)
	err = sigo.Enqueue(&data)

	if err != nil {
		fmt.Println("error in enqueuing jobs")
	} else {
		fmt.Println("success in enqueuing jobs")
	}
}

func consume(w http.ResponseWriter, r *http.Request) {
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

func config(w http.ResponseWriter, r *http.Request) {
	var data map[string]int

	b, err := ioutil.ReadAll(r.Body)
	defer r.Body.Close()

	err = json.Unmarshal(b, &data)
	if err != nil {
		return
	}

	// err = sigo.AddQueues()
}

func main() {
	// handle not found
	http.HandleFunc("/", ping)
	http.HandleFunc("/publish", publish)
	http.HandleFunc("/consume", consume)

	// http.HandleFunc("/config", config)

	fmt.Println("server listening on port 3000")

	http.ListenAndServe(":3000", nil)
}
