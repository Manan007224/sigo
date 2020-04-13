package manager

import (
	"fmt"
	"log"
	"sync"
	"time"

	pb "github.com/Manan007224/sigo/pkg/proto"
	"github.com/Manan007224/sigo/pkg/store"
)

// Manager is the interface which the scheduler uses to move around the jobs in redis
// queues and execute some other client operations such as Fail, Acknowledge or Push.
type Manager struct {
	Store *store.Store
	wg    *sync.WaitGroup
}

func NewManager(queueConfig []*pb.QueueConfig) (*Manager, error) {
	store, err := store.NewStore(queueConfig)
	return &Manager{Store: store}, err
}

// The scheduler is gRPC server and interacts often with the client, so any calls the scheduler
// makes to the client should happen in a separte goroutine so the server doesn't block the RPC call.
func (m *Manager) Do(task func() error) {
	m.wg.Add(1)
	go func(wg *sync.WaitGroup) {
		defer wg.Done()
		err := task()
		// TODO - find a better logging solution here.
		if err != nil {
			log.Println("[ERROR] in task execution", err)
		}
	}(m.wg)
}

func (m *Manager) Flush() {
	m.Store.Flush()
}

func (m *Manager) Shutdown() {
	m.wg.Wait()
	m.Store.Close()
}

func (m *Manager) Push(job *pb.JobPayload) error {
	job, err := m.validate(job)
	if err != nil {
		return err
	}

	if job.EnqueueAt == 0 {
		return m.Store.Queues[job.Queue].Add(job)
	}
	return m.Store.Scheduled.Add(job)
}

func (m *Manager) Fail(job *pb.FailPayload) error {
	if len(job.ErrorType) == 0 {
		job.ErrorMessage = "unknown"
	}
	if len(job.Backtrace) > 20 {
		job.Backtrace = job.Backtrace[:20]
	}

	failJob, ok := m.Store.Cache.Load(job.Id)
	if !ok {
		return fmt.Errorf("job with %s not found", job.Id)
	}
	m.Store.Cache.Delete(job.Id)
	if failJob.(*pb.Execution).Expiry >= time.Now().Unix() {
		job, err := m.Store.Working.FindJobById(job.Id, failJob.(*pb.Execution).Expiry)
		if err != nil {
			return err
		}
		if job.Retry > 0 {
			job.Retry--

			// TODO - think about a better exponential retry enqueue logic.

			// No need to remove the job from the working queue.
			// The cron job will automatically remove the job.
			return m.Store.Retry.AddJob(job, time.Now().Add(-5*time.Second).Unix())
		}
	}
	return nil
}

func (m *Manager) Acknowledge(job *pb.JobPayload) error {
	ackJob, ok := m.Store.Cache.Load(job.Jid)
	if !ok {
		return fmt.Errorf("job with %s not found", job.Jid)
	}
	m.Store.Cache.Delete(job.Jid)
	if ackJob.(*pb.Execution).Expiry >= time.Now().Unix() {
		return m.Store.Working.Remove(job, ackJob.(*pb.Execution).Expiry)
	}
	log.Println("[ACK] received after job expiry time")
	return nil
}

func (m *Manager) Fetch(from string) error {
	fetchJob, err := m.Store.Queues[from].Remove()
	if err != nil {
		return err
	}
	if _, ok := m.Store.Cache.Load(fetchJob.Jid); ok {
		return fmt.Errorf("job with %s id already exists", fetchJob.Jid)
	}
	executionExpiry := time.Now().Add(time.Duration(fetchJob.ReserveFor) * time.Second).Unix()
	m.Store.Cache.Store(fetchJob.Jid, &pb.Execution{Expiry: executionExpiry})
	if err = m.Store.Working.AddJob(fetchJob, executionExpiry); err != nil {
		return err
	}
	return nil
}

// Move the jobs from scheduled -> enqueue queues.
func (m *Manager) ProcessScheduledJobs(till int64) error {
	jobs, err := m.Store.Scheduled.Get(till)
	if err != nil {
		log.Println("[ProcessScheduledJobs] error")
		return err
	}
	for _, job := range jobs {
		if err = m.Store.Queues[job.Queue].Add(job); err != nil {
			log.Println("[ProcessScheduledJobs] error")
		}
	}
	return nil
}

// Move the jobs from working -> retry or done queue
func (m *Manager) ProcessExecutingJobs(till int64) error {
	jobs, err := m.Store.Working.Get(till)
	if err != nil {
		log.Println("[ProcessExecutingJobs] error")
		return err
	}

	for _, job := range jobs {
		// check if the client has ACKed or Failed the job or not.
		if _, ok := m.Store.Cache.Load(job.Jid); ok {
			if err = m.Store.Retry.Add(job); err != nil {
				log.Println("[ProcessExecutingJobs] error")
			}
		}
	}
	return nil
}

func (m *Manager) ProcessFailedJobs(till int64) error {
	jobs, err := m.Store.Retry.Get(till)
	if err != nil {
		log.Println("[ProcessFailedJobs] error")
		return err
	}

	for _, job := range jobs {
		if job.Retry == 0 {
			continue
		}
		if _, ok := m.Store.Cache.Load(job.Jid); ok {
			if err = m.Store.Queues[job.Queue].Add(job); err != nil {
				log.Println("[ProcessFailedJobs] error")
			}
		}
	}

	return nil
}

func (m *Manager) validate(job *pb.JobPayload) (*pb.JobPayload, error) {
	if len(job.Jid) == 0 {
		return nil, fmt.Errorf("invalid job id")
	}
	if len(job.Name) == 0 {
		return nil, fmt.Errorf("invalid job name")
	}
	if len(job.Queue) == 0 {
		return nil, fmt.Errorf("job queue not specified")
	}
	if job.Retry == 0 {
		job.Retry = 2
	}
	if job.ReserveFor == 0 {
		// Reserve each job for 30s if not specified
		job.ReserveFor = 30
	}
	return job, nil
}
