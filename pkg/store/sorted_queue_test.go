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

func createJob(id string, timestamp int64) *pb.JobPayload {
	params := make(map[string]*pb.Param)
	params["value"] = &pb.Param{Type: "int32", Value: []byte("55")}
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
			for i := 0; i < 30; i++ {
				err = queue.Add(createJob(strconv.Itoa(i), tm))
				Expect(err).ShouldNot(HaveOccurred())
			}
			count, err := queue.Size()
			Expect(count).Should(Equal(int64(30)))

			keyCount, err := queue.SizeByKey(tm)
			Expect(keyCount).Should(Equal(int64(30)))

			// Test find
			tm1 := time.Now().Add(1 * time.Minute).Unix()
			err = queue.Add(createJob(strconv.Itoa(123), tm1))
			count, err = queue.Size()
			Expect(count).Should(Equal(int64(31)))
			ok, err := queue.Find("123", tm1)
			Expect(ok).Should(BeTrue())

			// Test Remove
			err = queue.Remove("123", tm1)
			count, err = queue.Size()
			keyCount, err = queue.SizeByKey(tm1)

			Expect(keyCount).Should(Equal(int64(0)))
			Expect(count).Should(Equal(int64(30)))

			for i := 0; i < 30; i++ {
				err = queue.Remove(strconv.Itoa(i), tm)
				Expect(err).ShouldNot(HaveOccurred())
			}

			count, err = queue.Size()
			Expect(count).Should(Equal(int64(0)))

			// MoveTo function
			currentTm := time.Now()
			for i := 0; i < 30; i++ {
				duration := time.Duration(i) * time.Second
				err = queue.Add(createJob(strconv.Itoa(i), currentTm.Add(-duration).Unix()))
				Expect(err).ShouldNot(HaveOccurred())
			}
			count, err = queue.Size()
			Expect(count).Should(Equal(int64(30)))

			err = queue.MoveToSorted("test1", currentTm.Unix())
			queue1 := &SortedQueue{
				Client: client,
				Name:   "test1",
			}

			count, err = queue1.Size()
			Expect(count).Should(Equal(int64(30)))

			Expect(err).ShouldNot(HaveOccurred())
		})
	})
})
