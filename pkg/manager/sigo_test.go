package manager_test

import (
	"strconv"
	"time"

	. "github.com/Manan007224/sigo/pkg/manager"
	pb "github.com/Manan007224/sigo/pkg/proto"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func CreateJob(id string, timestamp int64, queue string) *pb.JobPayload {
	params := make(map[string]*pb.Param)
	params["value"] = &pb.Param{Type: "int32", Value: []byte("55")}
	return &pb.JobPayload{
		Jid:        id,
		Name:       "test-job",
		Args:       params,
		Retry:      int32(2),
		EnqueueAt:  timestamp,
		ReserveFor: int64(5),
		Queue:      queue,
	}
}

var _ = Describe("Sigo", func() {
	var (
		mgr *Manager
		err error
	)
	BeforeEach(func() {
		queues := []*pb.QueueConfig{}
		queues = append(queues, &pb.QueueConfig{Name: "High", Priority: 4})
		queues = append(queues, &pb.QueueConfig{Name: "Low", Priority: 2})
		mgr, err = NewManager(queues)
		Expect(err).ShouldNot(HaveOccurred())
	})

	AfterEach(func() {
		mgr.Flush()
	})

	Context("General functions", func() {
		It("manager functions", func() {
			// Test Push
			tm := time.Now().Unix()
			highJob := CreateJob("1", tm, "High")
			err = mgr.Push(highJob)
			count, err := mgr.Store.Scheduled.SizeByKey(tm)
			Expect(count).Should(Equal(int64(1)))

			highJob = CreateJob("2", 0, "High")
			highJob1 := CreateJob("3", 0, "High")
			err = mgr.Push(highJob)
			count = mgr.Store.Queues["High"].Size()
			Expect(count).Should(Equal(int64(1)))

			// Test Fetch
			_, err = mgr.Fetch("High")
			count = mgr.Store.Queues["High"].Size()
			Expect(count).Should(Equal(int64(0)))
			count, err = mgr.Store.Working.Size()
			Expect(count).Should(Equal(int64(1)))
			_, ok := mgr.Store.Cache.Load(highJob.Jid)
			Expect(ok).Should(BeTrue())

			// Test Acknowledge
			err = mgr.Acknowledge(highJob)
			count, err = mgr.Store.Working.Size()
			Expect(count).Should(Equal(int64(0)))
			_, ok = mgr.Store.Cache.Load(highJob.Jid)
			Expect(ok).Should(BeFalse())

			err = mgr.Push(highJob1)
			_, err = mgr.Fetch("High")
			count, err = mgr.Store.Working.Size()
			Expect(count).Should(Equal(int64(1)))

			failJob := &pb.FailPayload{
				Id:           "3",
				ErrorMessage: "unknown error",
				ErrorType:    "unknown",
				Backtrace:    []string{"/line1", "/line2"},
			}
			err = mgr.Fail(failJob)
			count, err = mgr.Store.Retry.Size()
			Expect(count).Should(Equal(int64(1)))

			_, ok = mgr.Store.Cache.Load(highJob1.Jid)
			Expect(ok).Should(BeFalse())

			// Test ProcessScheduledJobs

			Expect(err).ShouldNot(HaveOccurred())

		})

		Specify("Process Scheduled Jobs", func() {
			tm := time.Now()
			for i := 0; i < 10; i++ {
				if i%2 == 0 {
					mgr.Push(CreateJob(strconv.Itoa(i), tm.Unix(), "Low"))
				} else {
					mgr.Push(CreateJob(strconv.Itoa(i), tm.Unix(), "High"))
				}
			}
			err = mgr.ProcessScheduledJobs(tm.Unix())
			Expect(mgr.Store.Queues["High"].Size()).Should(Equal(int64(5)))
			Expect(mgr.Store.Queues["Low"].Size()).Should(Equal(int64(5)))

			for i := 0; i < 5; i++ {
				if i%2 == 0 {
					mgr.Fetch("High")
					mgr.Fetch("Low")
				}
			}
			count, err := mgr.Store.Working.Size()
			Expect(count).Should(Equal(int64(6)))
			err = mgr.ProcessExecutingJobs(tm.Add(5 * time.Second).Unix())
			count, err = mgr.Store.Retry.Size()
			Expect(count).Should(Equal(int64(6)))

			err = mgr.ProcessFailedJobs(tm.Add(5 * time.Second).Unix())
			count, err = mgr.Store.Retry.Size()
			Expect(count).Should(Equal(int64(0)))

			Expect(err).ShouldNot(HaveOccurred())
		})
	})
})
