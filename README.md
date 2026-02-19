# TigerScope 

TigerScope is a lightweight observability and telemetry analytics prototype designed to explore modern, open data infrastructure patterns.

##  Motivation

Traditional observability platforms often:

- Lock data into proprietary systems
- Require expensive aggregation pipelines
- Obscure raw telemetry access

TigerScope experiments with a different approach:

👉 Store raw telemetry in open formats  
👉 Persist to object storage  
👉 Query dynamically using DuckDB  

---

##  Architecture

Load Generator → Ingestion API → Kafka → Writer Consumer → Parquet → DuckDB → Query API → React Dashboard

---

##  Tech Stack

- **Go** – Backend services
- **Kafka** – Event streaming
- **MinIO** – S3-compatible object storage
- **Parquet** – Columnar analytics format
- **DuckDB** – OLAP query engine
- **React (Vite)** – Dashboard UI
- **Docker Compose** – Local infra orchestration

---

##  Features

- Streaming telemetry ingestion
- Kafka-based decoupled pipeline
- Parquet persistence
- DuckDB analytics over object storage
- Customer-centric reliability metrics
- Minimal React dashboard

---

##  Future Improvements

- Time-window filtering
- Alerting & anomaly detection
- Apache Iceberg integration
- Schema evolution
- Multi-tenant isolation
- Query caching

---

##  Purpose

Built as an educational prototype to explore:

- Open telemetry analytics
- Storage-first architectures
- Cost-efficient observability patterns

## 📸 Screenshots

### Dashboard
![Dashboard](/Users/ashritkommireddy/Desktop/Screenshot\ 2026-02-18\ at\ 7.59.11 PM.png)

### DuckDB Querying Parquet
![DuckDB](/Users/ashritkommireddy/Desktop/Screenshot\ 2026-02-18\ at\ 7.59.26 PM.png)


