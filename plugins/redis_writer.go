package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

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

// Automatic initialization when plugin is loaded
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
		// Convert result (Gmaps Entry) to JSON
		data, err := json.Marshal(result.Data)
		if err != nil {
			fmt.Printf("❌ Error serializing data: %v\n", err)
			continue
		}

		// Send to Redis (RPUSH for FIFO queue)
		// Use context.Background() to ensure sending isn't prematurely cancelled
		err = w.client.RPush(context.Background(), w.queue, data).Err()
		if err != nil {
			fmt.Printf("❌ Error writing to Redis: %v\n", err)
		} else {
            // Optional: Visual feedback
			// fmt.Print(".") 
		}
	}
	return nil
}
