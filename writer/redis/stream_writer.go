package rediswriter

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/google/uuid"
	"github.com/gosom/scrapemate"
	"github.com/redis/go-redis/v9"
)

// Configuración
const (
	StreamKey    = "gmaps_stream"
	PayloadLimit = 100 * 1024          // 100KB
	StoragePath  = "/app/storage/temp" // Volumen compartido
	HeartbeatTTL = 10 * time.Second
)

type StreamWriter struct {
	client *redis.Client
	jobID  string
}

// Payload estructura para Python
type StreamPayload struct {
	Type      string      `json:"type"` // "data" o "file_ref"
	JobID     string      `json:"job_id"`
	Data      interface{} `json:"data,omitempty"`
	FilePath  string      `json:"file_path,omitempty"`
	Timestamp string      `json:"timestamp"`
}

func NewStreamWriter(addr string, jobID string) *StreamWriter {
	if addr == "" {
		addr = "redis:6379"
	}

	// Asegurar directorio temporal
	if err := os.MkdirAll(StoragePath, 0755); err != nil {
		log.Printf("⚠️ Warning: Could not create storage path: %v", err)
	}

	return &StreamWriter{
		client: redis.NewClient(&redis.Options{Addr: addr}),
		jobID:  jobID,
	}
}

// StartHeartbeat inicia una goroutine que envía latidos
func (w *StreamWriter) StartHeartbeat(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(3 * time.Second)
		defer ticker.Stop()

		key := fmt.Sprintf("heartbeat:%s", w.jobID)

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				w.client.Set(ctx, key, time.Now().Unix(), HeartbeatTTL)
			}
		}
	}()
}

// LogHook permite enviar logs a Redis Pub/Sub
func (w *StreamWriter) Write(p []byte) (n int, err error) {
	msg := string(p)
	// Fire and forget log
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	w.client.Publish(ctx, fmt.Sprintf("job_logs:%s", w.jobID), msg)
	return len(p), nil
}

func (w *StreamWriter) Run(ctx context.Context, in <-chan scrapemate.Result) error {
	w.StartHeartbeat(ctx)
	log.Printf("🚀 Redis Stream Writer started for Job %s", w.jobID)

	for result := range in {
		// Preparar datos (mapeo similar al anterior plugin pero simplificado)
		dataBytes, err := json.Marshal(result.Data)
		if err != nil {
			log.Printf("❌ Serialization error: %v", err)
			continue
		}

		payload := StreamPayload{
			Type:      "data",
			JobID:     w.jobID,
			Timestamp: time.Now().Format(time.RFC3339),
		}

		// Lógica de Payload Gigante
		if len(dataBytes) > PayloadLimit {
			fileName := fmt.Sprintf("%s_%s.json", w.jobID, uuid.New().String())
			fullPath := filepath.Join(StoragePath, fileName)

			if err := os.WriteFile(fullPath, dataBytes, 0644); err != nil {
				log.Printf("❌ Failed to write large payload: %v", err)
				continue
			}

			payload.Type = "file_ref"
			payload.FilePath = fullPath
		} else {
			// Desempaquetar para enviar como JSON puro dentro del mensaje
			var rawData interface{}
			json.Unmarshal(dataBytes, &rawData)
			payload.Data = rawData
		}

		finalBytes, _ := json.Marshal(payload)

		// Escribir en Redis Stream (XADD)
		err = w.client.XAdd(ctx, &redis.XAddArgs{
			Stream: StreamKey,
			Values: map[string]interface{}{
				"payload": finalBytes,
				"job_id":  w.jobID,
			},
		}).Err()

		if err != nil {
			log.Printf("❌ Redis XADD error: %v", err)
		}
	}
	return nil
}
