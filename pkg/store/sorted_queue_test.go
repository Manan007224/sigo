package store_test

import (
	"strconv"
	"time"

	pb "github.com/Manan007224/sigo/pkg/proto"
	. "github.com/Manan007224/sigo/pkg/store"

	"github.com/go-redis/redis"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func CreateJob(id string, timestamp int64) *pb.JobPayload {
	params := []byte("params")
	return &pb.JobPayload{
		Jid:        id,
		Name:       "test-job",
		Args:       params,
		Retry:      int32(1),
		EnqueueAt:  timestamp,
		ReserveFor: int64(5),
		Queue:      "test",
	}
}

var _ = Describe("SortedQueue", func() {
	var (
		client *redis.Client
		err    error
		queue  *SortedQueue
	)

	BeforeEach(func() {
		client = redis.NewClient(&redis.Options{
			Addr: "localhost:6379",
			DB:   9,
		})
		queue = &SortedQueue{
			Name:   "test",
			Client: client,
		}
	})

	AfterEach(func() {
		client.FlushAll()
		client.Close()
	})

	Context("General functions", func() {
		It("sorted queue functions", func() {
			// Test Add
			tm := time.Now().Unix()
			jobs := []*pb.JobPayload{}
			for i := 0; i < 30; i++ {
				jobs = append(jobs, CreateJob(strconv.Itoa(i), tm))
				err = queue.Add(jobs[i])
				Expect(err).ShouldNot(HaveOccurred())
			}
			count, err := queue.Size()
			Expect(count).Should(Equal(int64(30)))

			keyCount, err := queue.SizeByScore(tm)
			Expect(keyCount).Should(Equal(int64(30)))

			// // Test AddWorkingJob
			// workq := &SortedQueue{Name: "working", Client: client}
			// for i := 0; i < 10; i++ {
			// 	if i%2 == 0 {
			// 		jobs[i].ClientId = "1"
			// 	} else {
			// 		jobs[i].ClientId = "2"
			// 	}
			// 	err = workq.AddWorkingJob(jobs[i], tm)
			// 	Expect(err).ShouldNot(HaveOccurred())
			// }
			// count, err = workq.Size()
			// Expect(count).Should(Equal(int64(10)))

			// Test Get
			jobs1, err := queue.Get(tm)
			Expect(jobs1).Should(HaveLen(30))

			// Test find
			tm1 := time.Now().Add(1 * time.Minute).Unix()
			testJob := CreateJob(strconv.Itoa(123), tm1)
			err = queue.Add(testJob)
			count, err = queue.Size()
			Expect(count).Should(Equal(int64(31)))
			ok, err := queue.Find("123", tm1)
			Expect(ok).Should(BeTrue())

			// Test Remove
			err = queue.Remove(testJob)
			count, err = queue.Size()
			keyCount, err = queue.SizeByScore(tm1)

			Expect(keyCount).Should(Equal(int64(0)))
			Expect(count).Should(Equal(int64(30)))
			count, err = queue.Size()

			for _, job := range jobs {
				err = queue.Remove(job)
				Expect(err).ShouldNot(HaveOccurred())
			}

			count, err = queue.Size()
			Expect(count).Should(Equal(int64(0)))

			// MoveTo function
			testTm := time.Now()
			for i := 0; i < 30; i++ {
				duration := time.Duration(i) * time.Second
				err = queue.Add(CreateJob(strconv.Itoa(i), testTm.Add(-duration).Unix()))
				Expect(err).ShouldNot(HaveOccurred())
			}
			count, err = queue.Size()
			Expect(count).Should(Equal(int64(30)))

			// MoveToSorted function
			currentTm := time.Now()
			for i := 0; i < 30; i++ {
				duration := time.Duration(i) * time.Second
				err = queue.Add(CreateJob(strconv.Itoa(i), currentTm.Add(-duration).Unix()))
				Expect(err).ShouldNot(HaveOccurred())
			}
			count, err = queue.Size()
			//Expect(count).Should(Equal(int64(30)))

			queue1 := &SortedQueue{
				Client: client,
				Name:   "test1",
			}
			err = queue.MoveToSorted(queue1.Name, currentTm.Unix())

			count, err = queue1.Size()
			Expect(count).Should(Equal(int64(30)))

			Expect(err).ShouldNot(HaveOccurred())
		})
	})
})
