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
			for i := 0; i < 30; i++ {
				tm := time.Now().Unix()
				err = queue.Add(createJob(strconv.Itoa(i), tm))
				Expect(err).ShouldNot(HaveOccurred())
			}
			count, err := queue.Size()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(count).Should(Equal(int64(30)))
		})
	})
})
