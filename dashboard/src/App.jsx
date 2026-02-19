import { useEffect, useMemo, useState } from "react";
import "./App.css";

const API_BASE = import.meta.env.VITE_API_BASE || "http://localhost:8090";

function Card({ title, children }) {
  return (
    <div className="card">
      <div className="cardTitle">{title}</div>
      <div>{children}</div>
    </div>
  );
}

function Table({ columns, rows }) {
  return (
    <div className="tableWrap">
      <table>
        <thead>
          <tr>
            {columns.map((c) => (
              <th key={c.key}>{c.label}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.length === 0 ? (
            <tr>
              <td colSpan={columns.length} className="muted">
                No data yet. Send events to /ingest.
              </td>
            </tr>
          ) : (
            rows.map((r, idx) => (
              <tr key={idx}>
                {columns.map((c) => (
                  <td key={c.key}>{r[c.key]}</td>
                ))}
              </tr>
            ))
          )}
        </tbody>
      </table>
    </div>
  );
}

async function fetchJSON(path) {
  const res = await fetch(`${API_BASE}${path}`);
  if (!res.ok) throw new Error(`${path} failed: ${res.status}`);
  return res.json();
}

export default function App() {
  const [loading, setLoading] = useState(true);
  const [err, setErr] = useState("");
  const [availability, setAvailability] = useState([]);
  const [topImpacted, setTopImpacted] = useState([]);
  const [errorRate, setErrorRate] = useState([]);
  const [p95, setP95] = useState([]);
  const [summary, setSummary] = useState(null);

  const [autoRefresh, setAutoRefresh] = useState(true);

  const refresh = async () => {
    setErr("");
    try {
      const [a, t, e, p, s] = await Promise.all([
        fetchJSON("/metrics/customer-availability"),
        fetchJSON("/metrics/top-impacted-customers"),
        fetchJSON("/metrics/error-rate"),
        fetchJSON("/metrics/p95-latency"),
        fetchJSON("/metrics/summary"),
      ]);
      setAvailability(a);
      setTopImpacted(t);
      setErrorRate(e);
      setP95(p);
      setSummary(s);
    } catch (e) {
      setErr(e.message || String(e));
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    refresh();
    // auto-refresh every 3 seconds
    if (!autoRefresh) return;
    const id = setInterval(refresh, 3000);
    return () => clearInterval(id);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [autoRefresh]);

  const headerMeta = useMemo(() => {
    if (!summary) return "—";
    return `${summary.total_rows} rows • latest_ingested: ${summary.latest_ingested}`;
  }, [summary]);

  return (
    <div className="page">
      <div className="topbar">
        <div>
          <div className="title">TigerScope</div>
          <div className="subtitle">
            Customer-Centric Observability • {headerMeta}
          </div>
        </div>

        <div className="actions">
          <label className="toggle">
            <input
              type="checkbox"
              checked={autoRefresh}
              onChange={(e) => setAutoRefresh(e.target.checked)}
            />
            Auto-refresh
          </label>
          <button onClick={refresh} className="btn">
            Refresh
          </button>
        </div>
      </div>

      {err ? <div className="error">API error: {err}</div> : null}
      {loading ? <div className="muted">Loading…</div> : null}

      <div className="grid">
        <Card title="Customer Availability (worst first)">
          <Table
            columns={[
              { key: "customer_id", label: "customer_id" },
              { key: "total", label: "total" },
              { key: "successful", label: "successful" },
              { key: "availability_pct", label: "availability_pct" },
            ]}
            rows={availability}
          />
          <div className="hint">
            This is the Firetiger view: who’s in the 0.1% experiencing pain.
          </div>
        </Card>

        <Card title="Top Impacted Customers (by errors)">
          <Table
            columns={[
              { key: "customer_id", label: "customer_id" },
              { key: "requests", label: "requests" },
              { key: "errors", label: "errors" },
            ]}
            rows={topImpacted}
          />
        </Card>

        <Card title="Error Rate by Service">
          <Table
            columns={[
              { key: "service", label: "service" },
              { key: "total_requests", label: "total_requests" },
              { key: "errors", label: "errors" },
              { key: "error_rate_pct", label: "error_rate_pct" },
            ]}
            rows={errorRate}
          />
        </Card>

        <Card title="p95 Latency by Service">
          <Table
            columns={[
              { key: "service", label: "service" },
              { key: "p95_latency_ms", label: "p95_latency_ms" },
            ]}
            rows={p95}
          />
        </Card>
      </div>

      <div className="footer muted">
        Data source: Parquet on object storage (MinIO) queried via DuckDB (httpfs).
      </div>
    </div>
  );
}
