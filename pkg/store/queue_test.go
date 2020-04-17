package store_test

import (
	"strconv"
	"time"

	. "github.com/Manan007224/sigo/pkg/store"

	"github.com/go-redis/redis"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Queue", func() {
	var (
		client  *redis.Client
		err     error
		queue   *Queue
		sortedQ *SortedQueue
	)

	BeforeEach(func() {
		client = redis.NewClient(&redis.Options{
			Addr: "localhost:6379",
			DB:   9,
		})
		queue = &Queue{
			Name:     "test",
			Client:   client,
			Priority: 1,
		}
		sortedQ = &SortedQueue{
			Name:   "sorted-test",
			Client: client,
		}
	})

	AfterEach(func() {
		client.FlushAll()
		client.Close()
	})

	Context("General Functions", func() {
		// Test Add
		It("Queue functions", func() {
			tm := time.Now().Unix()
			for i := 0; i < 30; i++ {
				err = queue.Add(CreateJob(strconv.Itoa(i), tm))
				Expect(err).ShouldNot(HaveOccurred())
			}
			count := queue.Size()
			Expect(count).Should(Equal(int64(30)))

			err = queue.MoveTo(sortedQ)
			count, err = sortedQ.Size()
			keyCount, err := sortedQ.SizeByScore(tm)

			Expect(count).Should(Equal(int64(1)))
			Expect(keyCount).Should(Equal(int64(1)))

			Expect(err).ShouldNot(HaveOccurred())
		})
	})
})
