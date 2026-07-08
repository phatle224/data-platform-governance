# Architecture — Data Platform Governance & Observability

## Overview

This document describes the technical architecture of the Data Platform Governance stack (Project B) and its integration patterns with the FMCG Real-Time Analytics Platform (Project A).

---

## Integration Model

Project B operates as a **sidecar governance layer** — it does not modify or interfere with Project A's data flow. Instead, it connects to the same Docker network as a passive observer and active cataloger.

```
Project A (fmcg-real-time-analytics)
    ├── Docker Network: fmcg_fmcg-network (bridge)
    ├── Databases: PostgreSQL, ClickHouse
    ├── Query Engine: Trino
    ├── Streaming: Kafka, Kafka Connect
    └── Monitoring: Prometheus, Grafana, cAdvisor

Project B (data-platform-governance)
    ├── Docker Network: gov-internal (bridge, isolated)
    ├── External Network: fmcg_fmcg-network (join as external)
    └── 4 Service Stacks:
        ├── openmetadata/ (Catalog & Lineage)
        ├── airflow/      (Orchestration & dbt)
        ├── alerting/     (SLO Monitoring)
        └── logging/      (Centralized Logs)
```

### Network Design

| Network | Scope | Purpose |
|---|---|---|
| `fmcg_fmcg-network` | Cross-project | Allows Project B to reach Project A databases by container hostname |
| `gov-internal` | Project B only | Internal communication between governance services (MySQL, ES, Airflow PG) |

**Services that join both networks:**
- `gov-openmetadata-server` — Scans Project A databases
- `gov-ingestion` — Runs metadata ingestion pipelines against Project A
- `gov-airflow-webserver` — Executes dbt against Project A PostgreSQL
- `gov-airflow-scheduler` — Schedules dbt runs against Project A PostgreSQL
- `gov-loki` — Accessible to Project A's Grafana for log queries

---

## Service Dependency Graph

```
                    ┌──────────────────┐
                    │   MySQL (OM DB)   │
                    └────────┬─────────┘
                             │
                    ┌────────▼─────────┐
                    │  Elasticsearch    │
                    └────────┬─────────┘
                             │
              ┌──────────────▼──────────────┐
              │   execute-migrate-all        │
              │   (Schema Migration, exits)  │
              └──────────────┬──────────────┘
                             │
              ┌──────────────▼──────────────┐
              │   OpenMetadata Server        │◄───── OpenLineage events
              │   (8585)                     │       from Airflow
              └──────────────┬──────────────┘
                             │
              ┌──────────────▼──────────────┐
              │   OM Ingestion (Airflow)     │
              │   (8081)                     │
              └─────────────────────────────┘

    ┌─────────────────┐
    │ Airflow Postgres │
    └────────┬────────┘
             │
    ┌────────▼────────┐
    │  airflow-init    │ (one-shot: db migrate + create admin)
    └────────┬────────┘
             │
    ┌────────▼────────┐     ┌───────────────────┐
    │ Airflow Webserver│     │ Airflow Scheduler  │
    │ (8082)           │     │ (emits StatsD)     │
    └─────────────────┘     └─────────┬──────────┘
                                      │ UDP :9125
                            ┌─────────▼──────────┐
                            │  StatsD Exporter    │
                            │  (9102 metrics)     │
                            └─────────┬──────────┘
                                      │ Scrape
                            ┌─────────▼──────────┐
                            │    Prometheus       │──── Alert Rules
                            │    (9095)           │
                            └─────────┬──────────┘
                                      │ Fire alerts
                            ┌─────────▼──────────┐
                            │   AlertManager      │──── Slack Webhook
                            │   (9093)            │
                            └────────────────────┘

    ┌────────────────────┐     ┌──────────────────┐
    │   Promtail         │────►│      Loki         │
    │   (Docker SD)      │     │      (3100)       │
    └────────────────────┘     └──────────────────┘
```

---

## Port Allocation Strategy

All ports are chosen to avoid conflicts with Project A's existing services:

| Port Range | Project | Services |
|---|---|---|
| 3000 | A | Grafana |
| 3100 | B | Loki |
| 8080 | A | Kafka UI |
| 8081 | B | OM Ingestion (Airflow) |
| 8082 | B | Airflow Webserver (dbt) |
| 8123 | A | ClickHouse HTTP |
| 8585-8586 | B | OpenMetadata Server + Admin |
| 9090 | A | Prometheus |
| 9093 | B | AlertManager |
| 9095 | B | Prometheus (Governance) |
| 9102 | B | StatsD Exporter |
| 13306 | B | MySQL (OM metadata) |
| 15433 | A | PostgreSQL |
| 15434 | B | Airflow Postgres |
| 19200 | B | Elasticsearch |

---

## Data Flow Patterns

### Pattern 1: Metadata Ingestion
```
Project A PostgreSQL/ClickHouse/Trino
    → OpenMetadata Ingestion Pipeline (scheduled)
    → OpenMetadata Server (catalog store in MySQL)
    → OpenMetadata UI (search, browse, data quality)
```

### Pattern 2: Transformation Lineage
```
Airflow Scheduler (cron: 06:00 UTC daily)
    → dbt deps / seed / run / test
    → dbt reads: Project A PostgreSQL (pos_transactions)
    → dbt writes: Project A PostgreSQL (analytics.stg_*, analytics.mart_*)
    → OpenLineage events → OpenMetadata Lineage Graph
```

### Pattern 3: SLO Alerting
```
Airflow Scheduler/Webserver
    → StatsD metrics (UDP :9125) → StatsD Exporter
    → Prometheus scrape (:9102, every 10s)
    → Alert rule evaluation (every 30s)
    → AlertManager → Slack Webhook
```

### Pattern 4: Centralized Logging
```
Docker containers (Project A + B)
    → Promtail (Docker socket discovery, auto-label)
    → Loki (HTTP push, TSDB storage)
    → Grafana (LogQL dashboard queries)
```
