package main

import (
	"context"
	"database/sql"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	_ "github.com/marcboeker/go-duckdb"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type QueryEngine struct {
	db          *sql.DB
	minioClient *minio.Client
	bucket      string
	prefix      string
	minioHTTP   string // e.g. http://localhost:9000
}

func main() {
	// DuckDB engine
	db, err := sql.Open("duckdb", "")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	// DuckDB httpfs config (we will read Parquet via HTTP URLs served by MinIO)
	mustExec(db, `INSTALL httpfs;`)
	mustExec(db, `LOAD httpfs;`)

	// (These S3 settings can stay; but we won't rely on s3:// paths in this demo)
	mustExec(db, `SET s3_endpoint='localhost:9000';`)
	mustExec(db, `SET s3_access_key_id='minioadmin';`)
	mustExec(db, `SET s3_secret_access_key='minioadmin';`)
	mustExec(db, `SET s3_use_ssl=false;`)
	mustExec(db, `SET s3_url_style='path';`)
	mustExec(db, `SET s3_region='us-east-1';`)

	// MinIO client (for listing objects)
	minioClient, err := minio.New("localhost:9000", &minio.Options{
		Creds:  credentials.NewStaticV4("minioadmin", "minioadmin", ""),
		Secure: false,
	})
	if err != nil {
		panic(err)
	}

	qe := &QueryEngine{
		db:          db,
		minioClient: minioClient,
		bucket:      "tigerscope",
		prefix:      "telemetry/parquet/",
		minioHTTP:   "http://localhost:9000",
	}

	e := echo.New()
	e.Use(middleware.CORS())

	e.GET("/healthz", func(c echo.Context) error {
		return c.String(http.StatusOK, "ok")
	})

	e.GET("/metrics/error-rate", qe.handleErrorRate)
	e.GET("/metrics/p95-latency", qe.handleP95Latency)
	e.GET("/metrics/top-impacted-customers", qe.handleTopImpactedCustomers)
	e.GET("/metrics/customer-availability", qe.handleCustomerAvailability)
	e.GET("/metrics/summary", qe.handleSummary)

	e.Logger.Fatal(e.Start(":8090"))
}

func mustExec(db *sql.DB, stmt string) {
	if _, err := db.Exec(stmt); err != nil {
		panic(err)
	}
}

func (qe *QueryEngine) parquetFileList(limit int) ([]string, error) {
	ctx := context.Background()

	opts := minio.ListObjectsOptions{
		Prefix:    qe.prefix,
		Recursive: true,
	}

	var files []string
	for obj := range qe.minioClient.ListObjects(ctx, qe.bucket, opts) {
		if obj.Err != nil {
			return nil, obj.Err
		}
		if strings.HasSuffix(obj.Key, ".parquet") {
			// Use HTTP URL so DuckDB reads via httpfs without S3 hostname inference
			files = append(files, qe.minioHTTP+"/"+qe.bucket+"/"+obj.Key)
		}
	}

	sort.Strings(files)

	if limit > 0 && len(files) > limit {
		files = files[len(files)-limit:]
	}

	return files, nil
}

func duckdbFileArrayLiteral(files []string) string {
	escaped := make([]string, 0, len(files))
	for _, f := range files {
		escaped = append(escaped, "'"+strings.ReplaceAll(f, "'", "''")+"'")
	}
	return "[" + strings.Join(escaped, ",") + "]"
}

func (qe *QueryEngine) handleErrorRate(c echo.Context) error {
	files, err := qe.parquetFileList(200)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]any{"error": err.Error()})
	}
	if len(files) == 0 {
		return c.JSON(http.StatusOK, []any{})
	}

	src := duckdbFileArrayLiteral(files)

	rows, err := qe.db.Query(`
		SELECT
		  service,
		  CAST(COUNT(*) AS BIGINT) AS total_requests,
		  CAST(SUM(CASE WHEN status_code >= 500 THEN 1 ELSE 0 END) AS BIGINT) AS errors,
		  CAST(ROUND(100.0 * SUM(CASE WHEN status_code >= 500 THEN 1 ELSE 0 END) / COUNT(*), 2) AS DOUBLE) AS error_rate_pct
		FROM read_parquet(` + src + `, filename=true)
		GROUP BY service
		ORDER BY error_rate_pct DESC;
	`)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]any{"error": err.Error()})
	}
	defer rows.Close()

	type Row struct {
		Service      string  `json:"service"`
		Total        int64   `json:"total_requests"`
		Errors       int64   `json:"errors"`
		ErrorRatePct float64 `json:"error_rate_pct"`
	}

	var out []Row
	for rows.Next() {
		var r Row
		if err := rows.Scan(&r.Service, &r.Total, &r.Errors, &r.ErrorRatePct); err != nil {
			return c.JSON(http.StatusInternalServerError, map[string]any{"error": err.Error()})
		}
		out = append(out, r)
	}

	return c.JSON(http.StatusOK, out)
}

func (qe *QueryEngine) handleP95Latency(c echo.Context) error {
	files, err := qe.parquetFileList(200)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]any{"error": err.Error()})
	}
	if len(files) == 0 {
		return c.JSON(http.StatusOK, []any{})
	}

	src := duckdbFileArrayLiteral(files)

	rows, err := qe.db.Query(`
		SELECT
		  service,
		  CAST(ROUND(quantile_cont(latency_ms, 0.95), 2) AS DOUBLE) AS p95_latency_ms
		FROM read_parquet(` + src + `, filename=true)
		GROUP BY service
		ORDER BY p95_latency_ms DESC;
	`)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]any{"error": err.Error()})
	}
	defer rows.Close()

	type Row struct {
		Service      string  `json:"service"`
		P95LatencyMs float64 `json:"p95_latency_ms"`
	}

	var out []Row
	for rows.Next() {
		var r Row
		if err := rows.Scan(&r.Service, &r.P95LatencyMs); err != nil {
			return c.JSON(http.StatusInternalServerError, map[string]any{"error": err.Error()})
		}
		out = append(out, r)
	}

	return c.JSON(http.StatusOK, out)
}

func (qe *QueryEngine) handleTopImpactedCustomers(c echo.Context) error {
	files, err := qe.parquetFileList(200)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]any{"error": err.Error()})
	}
	if len(files) == 0 {
		return c.JSON(http.StatusOK, []any{})
	}

	src := duckdbFileArrayLiteral(files)

	rows, err := qe.db.Query(`
		SELECT
		  customer_id,
		  CAST(COUNT(*) AS BIGINT) AS requests,
		  CAST(SUM(CASE WHEN status_code >= 500 THEN 1 ELSE 0 END) AS BIGINT) AS errors
		FROM read_parquet(` + src + `, filename=true)
		GROUP BY customer_id
		ORDER BY errors DESC
		LIMIT 10;
	`)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]any{"error": err.Error()})
	}
	defer rows.Close()

	type Row struct {
		CustomerID string `json:"customer_id"`
		Requests   int64  `json:"requests"`
		Errors     int64  `json:"errors"`
	}

	var out []Row
	for rows.Next() {
		var r Row
		if err := rows.Scan(&r.CustomerID, &r.Requests, &r.Errors); err != nil {
			return c.JSON(http.StatusInternalServerError, map[string]any{"error": err.Error()})
		}
		out = append(out, r)
	}

	return c.JSON(http.StatusOK, out)
}

func (qe *QueryEngine) handleCustomerAvailability(c echo.Context) error {
	files, err := qe.parquetFileList(200)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]any{"error": err.Error()})
	}
	if len(files) == 0 {
		return c.JSON(http.StatusOK, []any{})
	}

	src := duckdbFileArrayLiteral(files)

	rows, err := qe.db.Query(`
		SELECT
		  customer_id,
		  CAST(COUNT(*) AS BIGINT) AS total,
		  CAST(SUM(CASE WHEN status_code < 500 THEN 1 ELSE 0 END) AS BIGINT) AS successful,
		  CAST(ROUND(100.0 * SUM(CASE WHEN status_code < 500 THEN 1 ELSE 0 END) / COUNT(*), 2) AS DOUBLE) AS availability_pct
		FROM read_parquet(` + src + `, filename=true)
		GROUP BY customer_id
		ORDER BY availability_pct ASC;
	`)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]any{"error": err.Error()})
	}
	defer rows.Close()

	type Row struct {
		CustomerID      string  `json:"customer_id"`
		Total           int64   `json:"total"`
		Successful      int64   `json:"successful"`
		AvailabilityPct float64 `json:"availability_pct"`
	}

	var out []Row
	for rows.Next() {
		var r Row
		if err := rows.Scan(&r.CustomerID, &r.Total, &r.Successful, &r.AvailabilityPct); err != nil {
			return c.JSON(http.StatusInternalServerError, map[string]any{"error": err.Error()})
		}
		out = append(out, r)
	}

	return c.JSON(http.StatusOK, out)
}

func (qe *QueryEngine) handleSummary(c echo.Context) error {
	files, err := qe.parquetFileList(200)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]any{"error": err.Error()})
	}
	if len(files) == 0 {
		return c.JSON(http.StatusOK, map[string]any{"total_rows": 0, "latest_ingested": ""})
	}

	src := duckdbFileArrayLiteral(files)

	row := qe.db.QueryRow(`
		SELECT
		  CAST(COUNT(*) AS BIGINT) AS total_rows,
		  MAX(ingested_at) AS max_ingested_at
		FROM read_parquet(` + src + `, filename=true);
	`)

	var total int64
	var maxIngested time.Time
	if err := row.Scan(&total, &maxIngested); err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]any{"error": err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]any{
		"total_rows":      total,
		"latest_ingested": maxIngested.UTC().Format(time.RFC3339),
	})
}
