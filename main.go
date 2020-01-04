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
	"math/rand"
	"sync"
	"github.com/Sirupsen/logrus"
	"os"
	"strconv"
)

const SCHEDULED_QUEUE = "scheduled"

// Sigo ..
type Sigo struct {
	workers 		[]*Worker
	incomingJobs 	chan *Job
	pool		 	*redis.Pool
	jobQueues	 	map[string]int
	queueChooser	*Chooser
	freeWorkers		*hashset.Set
	busyWorkers		*hashset.Set
	mtx				sync.Mutex
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
	logger = NewLogger()
)

func NewSigo() *Sigo {
	return &Sigo {
		workers:      []*Worker{},
		incomingJobs: make(chan *Job),
		pool:		  initPool(),
		freeWorkers:  hashset.New(),
		busyWorkers:  hashset.New(),
	}
}

func NewLogger() *logrus.Logger {
	logger := logrus.New()
	logger.Out = os.Stdout
	return logger
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

func (sigo *Sigo) Decode(data []byte) (*Job, error) {
	var b Job
	if err := json.Unmarshal(data, &b); err != nil {
		return nil, fmt.Errorf("decode data failed: %s", err)
	}

	return &b, nil
}

func (sigo *Sigo) Enqueue(queue string, job *Job) error {
	conn := sigo.pool.Get()
	defer conn.Close()

	value, err := sigo.Encode(job)
	if err != nil {
		return fmt.Errorf("enqueue job failed: %s", err)
	}

	// TODO - failover mechanism
	_, err = conn.Do("RPUSH", queue, value)
	if err != nil {
		fmt.Errorf("enqueue failed for the job: %s", job)
	}
	return nil
}

func (sigo *Sigo) AddQueues(queues map[string]int) {
	var choices []Choice
	for k, v := range queues {
		choices = append(choices, Choice{Item: k, Weight: uint(v)})
	}
	sigo.queueChooser = NewChooser(choices)
}

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

func (sigo *Sigo) ZRangeByScore(timestamp int64) ([]string, error) {
	conn := sigo.pool.Get()
	defer conn.Close()

	data, err := redis.Strings(conn.Do("ZRANGEBYSCORE", SCHEDULED_QUEUE, "-inf", timestamp))
	if err != nil {
		return nil, fmt.Errorf("errror in finding all the jobs: %s", err)
	}
	return data, nil
}

func (sigo *Sigo) ZRemByScore(timestamp int64) ([]string, error) {
	conn := sigo.pool.Get()
	defer conn.Close()

	jobs, err := sigo.ZRangeByScore(timestamp)
	if err != nil {
		return nil, fmt.Errorf("error in zrangebyscore: %s", err)
	}

	// remove all the jobs
	_, err = conn.Do("ZREMRANGEBYSCORE", SCHEDULED_QUEUE, "-inf", timestamp)
	if err != nil {
		return nil, fmt.Errorf("error in zremrangebyscore: %s", err)
	}

	return jobs, nil
}	

func (sigo *Sigo) Zadd(queue string, key int64, value *Job) error {
	conn := sigo.pool.Get()
	defer conn.Close()

	job, err := sigo.Encode(value)
	if err != nil {
		return fmt.Errorf("error in encoding job: %s", value)
	}

	_, err = conn.Do("ZADD", queue, key, job)
	if err != nil {
		fmt.Errorf("error in pushing job: %s", err)
	}
	return nil
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

func (sigo *Sigo) AddFreeWorker(worker *Worker) {
	sigo.mtx.Lock()
	sigo.freeWorkers.Add(worker)
	sigo.mtx.Unlock()
}

func publish(w http.ResponseWriter, r *http.Request) {
	var data Job

	b, err := ioutil.ReadAll(r.Body)
	defer r.Body.Close()

	err = json.Unmarshal(b, &data)
	if err != nil {
		log.Println(err)
	}

	if data.Enqueue_at != "" {
		key, _ := strconv.ParseInt(data.Enqueue_at, 10, 64)
		err = sigo.Zadd("scheduled", key, &data)
	} else {
		err = sigo.Enqueue(data.Queue, &data)
	}

	if err != nil {
		log.Println("error in enqueuing jobs")
	} else {
		if data.Enqueue_at != "" { 
			data.QueueLog("scheduled")
		} else {
			data.QueueLog(data.Queue)
		}
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
		logger:		   logger,
	}

	sigo.AddFreeWorker(worker)
	go worker.Run()
}

func ping(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "route %s doesn't exist", r.URL.Path[1:])
}

// A standart config body would look like :- 
// { "high": "4", "normal": "2", "low": "1" }

func config(w http.ResponseWriter, r *http.Request) {
	var data map[string]int

	b, err := ioutil.ReadAll(r.Body)
	defer r.Body.Close()

	err = json.Unmarshal(b, &data)
	if err != nil {
		return
	}

	sigo.AddQueues(data)
}

func startDispatcher() {
	dispatcher := NewDispatcher(sigo, logger)
	for {
		if(sigo.queueChooser != nil) {
			dispatcher.Start()
			break
		}
	}
}

func main() {

	rand.Seed(time.Now().UTC().UnixNano())
	
	go startDispatcher()

	http.HandleFunc("/", ping)
	http.HandleFunc("/publish", publish)
	http.HandleFunc("/consume", consume)
	http.HandleFunc("/config", config)

	fmt.Println("server listening on port 3000")
	http.ListenAndServe(":3000", nil)
}
