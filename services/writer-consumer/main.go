package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/IBM/sarama"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/writer"
)

type TelemetryEvent struct {
	Timestamp   int64             `parquet:"name=timestamp, type=INT64, convertedtype=TIMESTAMP_MILLIS" json:"timestamp"`
	Service     string            `parquet:"name=service, type=BYTE_ARRAY, convertedtype=UTF8" json:"service"`
	CustomerID  string            `parquet:"name=customer_id, type=BYTE_ARRAY, convertedtype=UTF8" json:"customer_id"`
	Endpoint    string            `parquet:"name=endpoint, type=BYTE_ARRAY, convertedtype=UTF8" json:"endpoint"`
	Method      string            `parquet:"name=method, type=BYTE_ARRAY, convertedtype=UTF8" json:"method"`
	StatusCode  int32             `parquet:"name=status_code, type=INT32" json:"status_code"`
	LatencyMs   int32             `parquet:"name=latency_ms, type=INT32" json:"latency_ms"`
	TraceID     string            `parquet:"name=trace_id, type=BYTE_ARRAY, convertedtype=UTF8" json:"trace_id"`
	Error       string            `parquet:"name=error, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY, repetitiontype=OPTIONAL" json:"error,omitempty"`
	Environment string            `parquet:"name=environment, type=BYTE_ARRAY, convertedtype=UTF8" json:"environment"`
	SchemaVer   int32             `parquet:"name=schema_version, type=INT32" json:"schema_version"`
	IngestedAt  int64             `parquet:"name=ingested_at, type=INT64, convertedtype=TIMESTAMP_MILLIS" json:"ingested_at"`
	// NOTE: attributes map omitted for now (we’ll add later as a “wow” improvement)
}

type rawEvent struct {
	Timestamp   string            `json:"timestamp"`
	Service     string            `json:"service"`
	CustomerID  string            `json:"customer_id"`
	Endpoint    string            `json:"endpoint"`
	Method      string            `json:"method"`
	StatusCode  int32             `json:"status_code"`
	LatencyMs   int32             `json:"latency_ms"`
	TraceID     string            `json:"trace_id"`
	Error       string            `json:"error,omitempty"`
	Environment string            `json:"environment"`
	SchemaVer   int32             `json:"schema_version"`
	IngestedAt  string            `json:"ingested_at"`
	Attributes  map[string]string `json:"attributes,omitempty"`
}

type Config struct {
	KafkaBrokers string
	KafkaTopic   string
	KafkaGroup   string

	MinIOEndpoint  string
	MinIOAccessKey string
	MinIOSecretKey string
	MinIOBucket    string
	MinIOUseSSL    bool

	FlushEveryN    int
	FlushEverySecs int
}

func main() {
	cfg := Config{
		KafkaBrokers:   getenv("KAFKA_BROKERS", "localhost:9092"),
		KafkaTopic:     getenv("KAFKA_TOPIC", "telemetry.events"),
		KafkaGroup:     getenv("KAFKA_GROUP", "tigerscope-writer"),
		MinIOEndpoint:  getenv("MINIO_ENDPOINT", "localhost:9000"),
		MinIOAccessKey: getenv("MINIO_ACCESS_KEY", "minioadmin"),
		MinIOSecretKey: getenv("MINIO_SECRET_KEY", "minioadmin"),
		MinIOBucket:    getenv("MINIO_BUCKET", "tigerscope"),
		MinIOUseSSL:    getenv("MINIO_USE_SSL", "false") == "true",
		FlushEveryN:    getenvInt("FLUSH_EVERY_N", 500),
		FlushEverySecs: getenvInt("FLUSH_EVERY_SECS", 5),
	}

	minioClient, err := minio.New(cfg.MinIOEndpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.MinIOAccessKey, cfg.MinIOSecretKey, ""),
		Secure: cfg.MinIOUseSSL,
	})
	if err != nil {
		log.Fatalf("minio client error: %v", err)
	}

	ctx := context.Background()
	exists, err := minioClient.BucketExists(ctx, cfg.MinIOBucket)
	if err != nil {
		log.Fatalf("bucket check error: %v", err)
	}
	if !exists {
		if err := minioClient.MakeBucket(ctx, cfg.MinIOBucket, minio.MakeBucketOptions{}); err != nil {
			log.Fatalf("make bucket error: %v", err)
		}
	}

	log.Printf("writer-consumer(parquet) starting: kafka=%s topic=%s group=%s minio=%s bucket=%s",
		cfg.KafkaBrokers, cfg.KafkaTopic, cfg.KafkaGroup, cfg.MinIOEndpoint, cfg.MinIOBucket)

	consumerGroup, err := sarama.NewConsumerGroup(strings.Split(cfg.KafkaBrokers, ","), cfg.KafkaGroup, saramaConfig())
	if err != nil {
		log.Fatalf("kafka consumer group error: %v", err)
	}
	defer func() { _ = consumerGroup.Close() }()

	handler := NewWriterHandler(minioClient, cfg)

	for {
		if err := consumerGroup.Consume(ctx, []string{cfg.KafkaTopic}, handler); err != nil {
			log.Printf("consume error: %v", err)
			time.Sleep(1 * time.Second)
		}
	}
}

func saramaConfig() *sarama.Config {
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V2_8_0_0
	cfg.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRange
	cfg.Consumer.Offsets.Initial = sarama.OffsetNewest
	cfg.Consumer.Return.Errors = true
	cfg.ChannelBufferSize = 256
	return cfg
}

// --- Consumer Handler ---

type WriterHandler struct {
	minio *minio.Client
	cfg   Config

	events    []TelemetryEvent
	lastFlush time.Time
}

func NewWriterHandler(minioClient *minio.Client, cfg Config) *WriterHandler {
	return &WriterHandler{
		minio:      minioClient,
		cfg:        cfg,
		lastFlush:  time.Now(),
		events:     make([]TelemetryEvent, 0, cfg.FlushEveryN),
	}
}

func (h *WriterHandler) Setup(s sarama.ConsumerGroupSession) error {
	log.Printf("consumer setup: claims=%v", s.Claims())
	h.lastFlush = time.Now()
	return nil
}

func (h *WriterHandler) Cleanup(s sarama.ConsumerGroupSession) error {
	return h.flush(context.Background())
}

func (h *WriterHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	ticker := time.NewTicker(time.Duration(h.cfg.FlushEverySecs) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case msg, ok := <-claim.Messages():
			if !ok {
				return nil
			}

			ev, err := parseKafkaJSON(msg.Value)
			if err != nil {
				// Skip bad events but don't crash the pipeline
				log.Printf("bad event json (skipping): %v", err)
				sess.MarkMessage(msg, "")
				continue
			}

			h.events = append(h.events, ev)
			sess.MarkMessage(msg, "")

			if len(h.events) >= h.cfg.FlushEveryN {
				if err := h.flush(sess.Context()); err != nil {
					log.Printf("flush error: %v", err)
				}
			}

		case <-ticker.C:
			if time.Since(h.lastFlush) >= time.Duration(h.cfg.FlushEverySecs)*time.Second && len(h.events) > 0 {
				if err := h.flush(sess.Context()); err != nil {
					log.Printf("flush error: %v", err)
				}
			}

		case <-sess.Context().Done():
			return nil
		}
	}
}

func (h *WriterHandler) flush(ctx context.Context) error {
	if len(h.events) == 0 {
		return nil
	}

	now := time.Now().UTC()
	key := fmt.Sprintf("telemetry/parquet/date=%04d-%02d-%02d/hour=%02d/batch-%s.parquet",
		now.Year(), now.Month(), now.Day(), now.Hour(), randomHex(8))

	tmpDir := os.TempDir()
	tmpFile := filepath.Join(tmpDir, "tigerscope-"+randomHex(6)+".parquet")

	if err := writeParquet(tmpFile, h.events); err != nil {
		return fmt.Errorf("write parquet: %w", err)
	}

	fi, err := os.Stat(tmpFile)
	if err != nil {
		return err
	}

	f, err := os.Open(tmpFile)
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = h.minio.PutObject(ctx, h.cfg.MinIOBucket, key, f, fi.Size(), minio.PutObjectOptions{
		ContentType: "application/octet-stream",
	})
	if err != nil {
		return fmt.Errorf("upload to minio: %w", err)
	}

	log.Printf("flushed %d events -> s3://%s/%s (%d bytes)", len(h.events), h.cfg.MinIOBucket, key, fi.Size())

	_ = os.Remove(tmpFile)
	h.events = h.events[:0]
	h.lastFlush = time.Now()
	return nil
}

func writeParquet(path string, events []TelemetryEvent) error {
	fw, err := local.NewLocalFileWriter(path)
	if err != nil {
		return err
	}
	defer fw.Close()

	pw, err := writer.NewParquetWriter(fw, new(TelemetryEvent), 4)
	if err != nil {
		return err
	}
	pw.RowGroupSize = 128 * 1024 * 1024
	pw.PageSize = 8 * 1024
	pw.CompressionType = parquet.CompressionCodec_SNAPPY

	for _, ev := range events {
		if err := pw.Write(ev); err != nil {
			return err
		}
	}
	if err := pw.WriteStop(); err != nil {
		return err
	}
	return nil
}

func parseKafkaJSON(b []byte) (TelemetryEvent, error) {
	var r rawEvent
	if err := json.Unmarshal(b, &r); err != nil {
		return TelemetryEvent{}, err
	}

	// parse timestamps (RFC3339 from ingestion-api)
	ts, err := time.Parse(time.RFC3339Nano, r.Timestamp)
	if err != nil {
		ts = time.Now().UTC()
	}
	ing, err := time.Parse(time.RFC3339Nano, r.IngestedAt)
	if err != nil {
		ing = time.Now().UTC()
	}

	return TelemetryEvent{
		Timestamp:   ts.UnixMilli(),
		Service:     r.Service,
		CustomerID:  r.CustomerID,
		Endpoint:    r.Endpoint,
		Method:      r.Method,
		StatusCode:  r.StatusCode,
		LatencyMs:   r.LatencyMs,
		TraceID:     r.TraceID,
		Error:       r.Error,
		Environment: r.Environment,
		SchemaVer:   r.SchemaVer,
		IngestedAt:  ing.UnixMilli(),
	}, nil
}

func randomHex(nBytes int) string {
	b := make([]byte, nBytes)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func getenv(key, def string) string {
	v := os.Getenv(key)
	if strings.TrimSpace(v) == "" {
		return def
	}
	return v
}

func getenvInt(key string, def int) int {
	v := os.Getenv(key)
	if strings.TrimSpace(v) == "" {
		return def
	}
	var n int
	_, err := fmt.Sscanf(v, "%d", &n)
	if err != nil {
		return def
	}
	return n
}
