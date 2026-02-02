package rediswriter

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/gosom/scrapemate"
	"github.com/redis/go-redis/v9"
)

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

// PythonCompatibleEntry maps internal Go Entry to the strict schema expected by Python
type PythonCompatibleEntry struct {
	InputID       string  `json:"input_id"`
	Title         string  `json:"title"`
	Category      string  `json:"category"`
	Address       string  `json:"address"`
	Website       string  `json:"website"`
	Phone         string  `json:"phone"`
	ReviewCount   int     `json:"review_count"`
	ReviewRating  float64 `json:"review_rating"`
	Latitude      float64 `json:"latitude"`
	Longitude     float64 `json:"longitude"` // Fixed typo from 'longtitude'
	GoogleMapsURL string  `json:"google_maps_url"`
	PlaceID       string  `json:"google_maps_cid"` // Map PlaceID to cid in Python context
	SourceURL     string  `json:"url"`             // Canonical URL for enrichment
}

func New(addr, queueName string) *RedisWriter {
	if addr == "" {
		addr = "redis:6379"
	}
	if queueName == "" {
		queueName = "gmaps_discovery_queue"
	}
	return &RedisWriter{
		client: redis.NewClient(&redis.Options{
			Addr: addr,
		}),
		queue: queueName,
	}
}

func (w *RedisWriter) Run(ctx context.Context, in <-chan scrapemate.Result) error {
	// Ensure queue is available (optional debug log)
	// fmt.Printf("RedisWriter started for queue: %s\n", w.queue)

	for result := range in {
		var jobID string
		if result.Job != nil {
			jobID = result.Job.GetID()
		}

		// TRANSFORM: Basic Mapper to Python Schema
		// We use strict mapping to ensure Python Pydantic doesn't fail
		rawBytes, _ := json.Marshal(result.Data) // Temporary hack to access struct fields loosely if generic

		// Ideally we would cast result.Data to gmaps.Entry but we want to avoid cyclic import with 'gmaps' package if this writer is used elsewhere.
		// For now, let's assume result.Data creates a map[string]interface{} compatible JSON or use a Map helper.
		// BETTER APPROACH: Unmarshal to a temporary map to be safe and flexible.
		var dataMap map[string]interface{}
		_ = json.Unmarshal(rawBytes, &dataMap)

		var website string
		if val, ok := dataMap["web_site"].(string); ok {
			website = val
		}

		// Fix Longitude typo from Go struct (longtitude -> longitude)
		var longitude float64
		if val, ok := dataMap["longtitude"].(float64); ok {
			longitude = val
		} else if val, ok := dataMap["longitude"].(float64); ok {
			longitude = val
		}

		canonicalPayload := PythonCompatibleEntry{
			InputID:       fmt.Sprintf("%v", dataMap["input_id"]),
			Title:         fmt.Sprintf("%v", dataMap["title"]),
			Category:      fmt.Sprintf("%v", dataMap["category"]),
			Address:       fmt.Sprintf("%v", dataMap["address"]),
			Website:       website,
			Phone:         fmt.Sprintf("%v", dataMap["phone"]),
			GoogleMapsURL: fmt.Sprintf("%v", dataMap["link"]),
			SourceURL:     website, // Important: This is the key for enrichment
			Latitude:      dataMap["latitude"].(float64),
			Longitude:     longitude,
		}

		// Safe cast numbers
		if rc, ok := dataMap["review_count"].(float64); ok {
			canonicalPayload.ReviewCount = int(rc)
		}
		if rr, ok := dataMap["review_rating"].(float64); ok {
			canonicalPayload.ReviewRating = rr
		}

		payload := HandoverPayload{
			JobID:     jobID,
			Source:    "gmaps_go_web",
			Timestamp: time.Now().Format(time.RFC3339),
			Data:      canonicalPayload,
		}

		data, err := json.Marshal(payload)
		if err != nil {
			fmt.Printf("❌ Error serializing data: %v\n", err)
			continue
		}

		// Send to Redis (RPUSH for FIFO queue)
		// We use context.Background() extensively in writer usually, but better use passed ctx
		// However, if write needs to survive context cancellation for a few ms, maybe Background is better?
		// Stick to ctx for now.
		err = w.client.RPush(ctx, w.queue, data).Err()
		if err != nil {
			fmt.Printf("❌ Error writing to Redis: %v\n", err)
		}
	}
	return nil
}
