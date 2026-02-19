package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

type Event struct {
	Timestamp  string            `json:"timestamp"`
	Service    string            `json:"service"`
	CustomerID string            `json:"customer_id"`
	Endpoint   string            `json:"endpoint"`
	Method     string            `json:"method"`
	StatusCode int               `json:"status_code"`
	LatencyMs  int               `json:"latency_ms"`
	TraceID    string            `json:"trace_id"`
	Error      string            `json:"error,omitempty"`
	Attributes map[string]string `json:"attributes"`
}

func main() {
	url := "http://localhost:8081/ingest"

	services := []string{
		"auth-service",
		"payments-service",
		"orders-service",
	}

	customers := []string{
		"cust_200",
		"cust_201",
		"cust_202",
		"cust_203",
	}

	fmt.Println("🚀 Sending demo telemetry events...")

	for i := 1; i <= 50; i++ {
		event := Event{
			Timestamp:  time.Now().UTC().Format(time.RFC3339),
			Service:    services[i%len(services)],
			CustomerID: customers[i%len(customers)],
			Endpoint:   "/api/demo",
			Method:     "POST",
			StatusCode: pickStatus(i),
			LatencyMs:  50 + (i % 300),
			TraceID:    fmt.Sprintf("trace-%d", i),
			Attributes: map[string]string{
				"build":  "v0.1.0",
				"region": "us-east-1",
			},
		}

		if event.StatusCode >= 500 {
			event.Error = "simulated_failure"
		}

		sendEvent(url, event)
		time.Sleep(50 * time.Millisecond)
	}

	fmt.Println("✅ Done sending events")
}

func pickStatus(i int) int {
	if i%10 == 0 {
		return 500 // inject some failures
	}
	return 200
}

func sendEvent(url string, event Event) {
	body, _ := json.Marshal(event)

	resp, err := http.Post(url, "application/json", bytes.NewBuffer(body))
	if err != nil {
		fmt.Println("❌ Failed:", err)
		return
	}
	defer resp.Body.Close()

	fmt.Println("Sent:", event.Service, event.CustomerID, event.StatusCode)
}
