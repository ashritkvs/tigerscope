package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/IBM/sarama"
)

type TelemetryEvent struct {
	Timestamp   time.Time         `json:"timestamp"`
	Service     string            `json:"service"`
	CustomerID  string            `json:"customer_id"`
	Endpoint    string            `json:"endpoint"`
	Method      string            `json:"method"`
	StatusCode  int               `json:"status_code"`
	LatencyMs   int               `json:"latency_ms"`
	TraceID     string            `json:"trace_id"`
	Error       string            `json:"error,omitempty"`
	Attributes  map[string]string `json:"attributes,omitempty"`
	RequestID   string            `json:"request_id"`
	IngestedAt  time.Time         `json:"ingested_at"`
	SchemaVer   int               `json:"schema_version"`
	Environment string            `json:"environment"`
}

type Server struct {
	producer sarama.SyncProducer
	topic    string
}

func main() {
	kafkaBrokers := getenv("KAFKA_BROKERS", "localhost:9092")
	topic := getenv("KAFKA_TOPIC", "telemetry.events")
	port := getenv("PORT", "8080")
	env := getenv("ENVIRONMENT", "local")

	producer, err := newProducer(strings.Split(kafkaBrokers, ","))
	if err != nil {
		log.Fatalf("failed to create kafka producer: %v", err)
	}
	defer func() { _ = producer.Close() }()

	s := &Server{producer: producer, topic: topic}

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})
	mux.HandleFunc("/ingest", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()

		var ev TelemetryEvent
		dec := json.NewDecoder(r.Body)
		dec.DisallowUnknownFields()
		if err := dec.Decode(&ev); err != nil {
			http.Error(w, "invalid json: "+err.Error(), http.StatusBadRequest)
			return
		}

		// Fill defaults / enforce required fields
		now := time.Now().UTC()
		if ev.Timestamp.IsZero() {
			ev.Timestamp = now
		} else {
			ev.Timestamp = ev.Timestamp.UTC()
		}
		ev.IngestedAt = now
		ev.SchemaVer = 1
		ev.Environment = env

		if strings.TrimSpace(ev.Service) == "" ||
			strings.TrimSpace(ev.CustomerID) == "" ||
			strings.TrimSpace(ev.Endpoint) == "" ||
			strings.TrimSpace(ev.Method) == "" ||
			ev.StatusCode == 0 {
			http.Error(w, "missing required fields: service, customer_id, endpoint, method, status_code", http.StatusBadRequest)
			return
		}

		if ev.TraceID == "" {
			ev.TraceID = randomHex(16)
		}
		ev.RequestID = randomHex(12)

		b, err := json.Marshal(ev)
		if err != nil {
			http.Error(w, "marshal error", http.StatusInternalServerError)
			return
		}

		// Key by customer_id (keeps ordering per customer in Kafka partitions)
		msg := &sarama.ProducerMessage{
			Topic: s.topic,
			Key:   sarama.StringEncoder(ev.CustomerID),
			Value: sarama.ByteEncoder(b),
			Headers: []sarama.RecordHeader{
				{Key: []byte("service"), Value: []byte(ev.Service)},
				{Key: []byte("env"), Value: []byte(env)},
			},
			Timestamp: now,
		}

		partition, offset, err := s.producer.SendMessage(msg)
		if err != nil {
			http.Error(w, "kafka publish failed: "+err.Error(), http.StatusBadGateway)
			return
		}

		select {
		case <-ctx.Done():
			http.Error(w, "request timeout", http.StatusGatewayTimeout)
			return
		default:
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"status":    "accepted",
			"topic":     s.topic,
			"partition": partition,
			"offset":    offset,
			"trace_id":  ev.TraceID,
			"request_id": ev.RequestID,
		})
	})

	addr := ":" + port
	log.Printf("ingestion-api listening on %s (kafka=%s topic=%s env=%s)", addr, kafkaBrokers, topic, env)
	log.Fatal(http.ListenAndServe(addr, withLogging(mux)))
}

func newProducer(brokers []string) (sarama.SyncProducer, error) {
	cfg := sarama.NewConfig()
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Producer.Retry.Max = 5
	cfg.Producer.Return.Successes = true
	cfg.Producer.Idempotent = true
	cfg.Net.MaxOpenRequests = 1
	cfg.Version = sarama.V2_8_0_0

	return sarama.NewSyncProducer(brokers, cfg)
}

func withLogging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		log.Printf("%s %s %s", r.Method, r.URL.Path, time.Since(start))
	})
}

func getenv(key, def string) string {
	v := os.Getenv(key)
	if strings.TrimSpace(v) == "" {
		return def
	}
	return v
}

func randomHex(nBytes int) string {
	b := make([]byte, nBytes)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// optional helper if you want to parse ints from env later
func getenvInt(key string, def int) int {
	v := os.Getenv(key)
	if strings.TrimSpace(v) == "" {
		return def
	}
	i, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return i
}
