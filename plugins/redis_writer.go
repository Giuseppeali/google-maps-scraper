package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/gosom/scrapemate"
	"github.com/redis/go-redis/v9"
)

// Ensure we implement the interface
var _ scrapemate.ResultWriter = (*RedisWriter)(nil)

// Exported symbol that the scraper will look for. MUST be named 'Writer'
// It must be of type scrapemate.ResultWriter interface, so when Lookup returns *scrapemate.ResultWriter
// the type assertion works.
var Writer scrapemate.ResultWriter

type RedisWriter struct {
	client *redis.Client
	queue  string
}

type HandoverPayload struct {
	JobID     string      `json:"job_id"`
	Source    string      `json:"source"`
	Timestamp string      `json:"timestamp"`
	Data      interface{} `json:"data"`
}

func init() {
	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "redis:6379"
	}

	queueName := os.Getenv("REDIS_QUEUE_NAME")
	if queueName == "" {
		queueName = "gmaps_discovery_queue"
	}

	fmt.Printf("🔌 Redis Plugin Init: Connecting to %s (Queue: %s)\n", redisAddr, queueName)

	Writer = &RedisWriter{
		client: redis.NewClient(&redis.Options{
			Addr: redisAddr,
		}),
		queue: queueName,
	}
}

func (w *RedisWriter) Run(ctx context.Context, in <-chan scrapemate.Result) error {
	for result := range in {
		var jobID string
		if result.Job != nil {
			jobID = result.Job.GetID()
		}

		payload := HandoverPayload{
			JobID:     jobID,
			Source:    "gmaps_go",
			Timestamp: time.Now().Format(time.RFC3339),
			Data:      result.Data,
		}

		data, err := json.Marshal(payload)
		if err != nil {
			fmt.Printf("❌ Error serializing data: %v\n", err)
			continue
		}

		// Send to Redis (RPUSH for FIFO queue)
		err = w.client.RPush(context.Background(), w.queue, data).Err()
		if err != nil {
			fmt.Printf("❌ Error writing to Redis: %v\n", err)
		}
	}
	return nil
}

func main() {
	// Plugin entry point - empty
}
