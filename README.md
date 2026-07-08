<div>
  <img style="width: 100%" src="https://capsule-render.vercel.app/api?type=waving&height=120&section=header&reversal=true&text=Data%20Platform%20Governance&fontSize=30&fontColor=ffffff&fontAlign=50&fontAlignY=45&rotate=0&stroke=-&animation=twinkling&desc=OpenMetadata%20%E2%80%A2%20Airflow%20%2B%20dbt%20%E2%80%A2%20AlertManager%20%E2%80%A2%20Grafana%20Loki&descSize=15&descAlign=50&descAlignY=65&textBg=false&color=gradient" />
</div>

<div align="center">
  <strong>English</strong> | <a href="README_VI.md">Vietnamese</a>
</div>

<h3 align="center">Enterprise Data Governance, Lineage Tracking, SLO Alerting & Centralized Logging for FMCG Real-Time Analytics</h3>

<div align="center">
  <img src="https://img.shields.io/badge/Catalog-OpenMetadata-5B4FDB?style=for-the-badge&logo=data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAA4AAAAOCAYAAAAfSC3RAAAAAXNSR0IArs4c6QAAAIRlWElmTU0AKgAAAAgABQESAAMAAAABAAEAAAEaAAUAAAABAAAASgEbAAUAAAABAAAAUgEoAAMAAAABAAIAAIdpAAQAAAABAAAAWgAAAAAAAABIAAAAAQAAAEgAAAABAAOgAQADAAAAAQABAACgAgAEAAAAAQAAAA6gAwAEAAAAAQAAAA4AAAAA/xBOIAAAAAlwSFlzAAALEwAACxMBAJqcGAAAAVlpVFh0WE1MOmNvbS5hZG9iZS54bXAAAAAAADx4OnhtcG1ldGEgeG1sbnM6eD0iYWRvYmU6bnM6bWV0YS8iIHg6eG1wdGs9IlhNUCBDb3JlIDYuMC4wIj4KICAgPHJkZjpSREYgeG1sbnM6cmRmPSJodHRwOi8vd3d3LnczLm9yZy8xOTk5LzAyLzIyLXJkZi1zeW50YXgtbnMjIj4KICAgICAgPHJkZjpEZXNjcmlwdGlvbiByZGY6YWJvdXQ9IiIKICAgICAgICAgICAgeG1sbnM6dGlmZj0iaHR0cDovL25zLmFkb2JlLmNvbS90aWZmLzEuMC8iPgogICAgICAgICA8dGlmZjpPcmllbnRhdGlvbj4xPC90aWZmOk9yaWVudGF0aW9uPgogICAgICA8L3JkZjpEZXNjcmlwdGlvbj4KICAgPC9yZGY6UkRGPgo8L3g6eG1wbWV0YT4KGV7hBwAAABxJREFUKBVjZGBg+A9EQAAkMYoZMBRhU4NNDgBXcAEPK0OI8AAAAABJRU5ErkJggg==&logoColor=white" alt="openmetadata badge" />
  <img src="https://img.shields.io/badge/Orchestration-Apache%20Airflow-017CEE?style=for-the-badge&logo=apacheairflow&logoColor=white" alt="airflow badge" />
  <img src="https://img.shields.io/badge/Transform-dbt-FF694B?style=for-the-badge&logo=dbt&logoColor=white" alt="dbt badge" />
  <img src="https://img.shields.io/badge/Lineage-OpenLineage-1C1E21?style=for-the-badge&logo=linuxfoundation&logoColor=white" alt="openlineage badge" />
  <img src="https://img.shields.io/badge/Alerting-Prometheus-E6522C?style=for-the-badge&logo=prometheus&logoColor=white" alt="prometheus badge" />
  <img src="https://img.shields.io/badge/Logging-Grafana%20Loki-F2CC0C?style=for-the-badge&logo=grafana&logoColor=black" alt="loki badge" />
  <img src="https://img.shields.io/badge/Infra-Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white" alt="docker badge" />
</div>

---

## Table of Contents

1. [Project Overview](#project-overview)
2. [System Architecture & Data Flow](#system-architecture--data-flow)
3. [Core Features](#core-features)
4. [Tech Stack](#tech-stack)
5. [Directory Structure](#directory-structure)
6. [Quick Start Guide](#quick-start-guide)
7. [Service Endpoints](#service-endpoints)
8. [dbt Lineage Model](#dbt-lineage-model)
9. [Alert Rules & SLO Definitions](#alert-rules--slo-definitions)
10. [Troubleshooting](#troubleshooting)

---

## Project Overview

This project implements the **Governance & Observability layer** for the [FMCG Real-Time Analytics Platform](https://github.com/phatle224/fmcg-real-time-analytics) (Project A). It operates as a **companion repository** that connects to Project A's running infrastructure via a shared Docker network to provide:

1. **Data Catalog & Lineage** — OpenMetadata scans PostgreSQL, ClickHouse, and Trino schemas from Project A, while OpenLineage captures transformation lineage from dbt runs.
2. **Data Transformation** — dbt models clean and aggregate raw POS transaction data into analytics-ready mart tables, orchestrated by Apache Airflow.
3. **Pipeline SLO Alerting** — Prometheus scrapes Airflow DAG metrics via StatsD Exporter and routes critical alerts (DAG failures, SLO breaches) to Slack via AlertManager.
4. **Centralized Logging** — Loki + Promtail aggregate Docker container logs from both projects into a single queryable store, visualized on Grafana dashboards using LogQL.

### Problem → Solution → Result (PSR)

| Dimension | Description |
|---|---|
| **Problem** | Raw data assets in the analytics platform (Project A) lack documentation, lineage tracking, quality testing, and centralized operational monitoring. Debugging production issues requires SSH-ing into individual containers. |
| **Solution** | Deploy a governance stack that automatically catalogs all database schemas (OpenMetadata), tracks data lineage from raw → staging → mart (OpenLineage + dbt), monitors pipeline SLOs with threshold-based alerting (Prometheus + AlertManager → Slack), and centralizes all container logs (Loki + Promtail → Grafana). |
| **Result** | Full metadata catalog with automated lineage graph covering 3 data sources. Pipeline failures trigger Slack alerts within 30 seconds. Centralized log search reduces debugging time from container-level SSH to a single Grafana query. All 14 services orchestrated via `make up` with zero manual configuration. |

---

## System Architecture & Data Flow

This stack connects to Project A via the shared Docker network `fmcg_fmcg-network`, enabling container-to-container communication by hostname.

```
┌────────────────────────────────────────────────────────────────────────┐
│  PROJECT A (Data Infrastructure — FMCG Analytics Platform)             │
│  ┌─────────────┐ ┌──────────────┐ ┌─────────┐ ┌──────────────────┐   │
│  │ fmcg-postgres│ │fmcg-clickhouse│ │fmcg-trino│ │ fmcg-kafka       │   │
│  │  :5432       │ │  :8123        │ │  :8080   │ │  :29092          │   │
│  └──────┬───────┘ └──────┬───────┘ └────┬─────┘ └──────────────────┘   │
│         │                │              │                               │
│         └────────────────┼──────────────┘                               │
│                          │ Docker Network: fmcg_fmcg-network            │
└──────────────────────────┼──────────────────────────────────────────────┘
                           │ (Connect & Observe)
┌──────────────────────────┼──────────────────────────────────────────────┐
│  PROJECT B (Governance & Observability Stack)                           │
│                          │                                              │
│  ┌───────────────────────▼────────────────────────────────────────────┐ │
│  │                   DATA CATALOG & LINEAGE                          │ │
│  │  OpenMetadata ◄── Metadata Ingestion (Postgres, CH, Trino)        │ │
│  │       ▲                                                           │ │
│  │       │ OpenLineage Events                                        │ │
│  │  Airflow DAGs ──► dbt run ──► staging ──► mart tables             │ │
│  └───────────────────────────────────────────────────────────────────┘ │
│                                                                        │
│  ┌─────────────────────── PIPELINE SLO ALERTING ─────────────────────┐ │
│  │  Airflow ──► StatsD ──► Prometheus ──► AlertManager ──► Slack     │ │
│  │  (metrics)   (9125)     (9095)         (9093)                     │ │
│  └───────────────────────────────────────────────────────────────────┘ │
│                                                                        │
│  ┌─────────────────────── CENTRALIZED LOGGING ───────────────────────┐ │
│  │  Docker Containers ──► Promtail ──► Loki ──► Grafana Dashboard    │ │
│  │  (Project A + B)       (auto-discover)  (3100)   (LogQL panels)   │ │
│  └───────────────────────────────────────────────────────────────────┘ │
└────────────────────────────────────────────────────────────────────────┘
```

### Data Lineage Graph (dbt)

```
raw_fmcg.pos_transactions (Project A PostgreSQL)
    │
    └──► stg_pos_transactions (view)
         │   - Region name mapping (HN → Hà Nội, HCM → TP. Hồ Chí Minh...)
         │   - Time dimension extraction (date, hour, day_of_week)
         │   - Transaction tier classification (high/medium/low value)
         │
         ├──► mart_sales_by_region (table)
         │      Daily revenue, units, and transaction mix by region + category
         │
         └──► mart_product_performance (table)
                Product ranking by revenue within category, daily revenue share %
```

---

## Core Features

### 1. Automated Metadata Cataloging (OpenMetadata)
OpenMetadata connects directly to Project A's databases via the shared Docker network and automatically discovers table schemas, column types, and descriptions. Scheduled ingestion pipelines keep the catalog up-to-date without manual intervention.

### 2. End-to-End Data Lineage (OpenLineage + dbt)
When Airflow triggers `dbt run`, the OpenLineage provider automatically emits lineage events to OpenMetadata's HTTP listener. The result is a visual lineage graph tracing data flow from raw source tables through staging views to aggregated mart tables.

### 3. Pipeline SLO Monitoring & Slack Alerting (Prometheus + AlertManager)
Airflow emits operational metrics via StatsD (DAG duration, task success/failure counts, scheduler heartbeat). Prometheus evaluates alert rules against these metrics. When a DAG fails or exceeds the 5-minute SLO, AlertManager routes a formatted notification to the configured Slack channel within 30 seconds.

### 4. Centralized Log Aggregation (Loki + Promtail)
Promtail auto-discovers Docker containers from both projects using the Docker socket, extracts log streams, and tags them by project (`project-a` / `project-b`), service name, and detected log level. A pre-built Grafana dashboard provides 9 panels for real-time error monitoring and log volume analysis using LogQL.

---

## Tech Stack

### Data Catalog & Lineage
<div align="left">
  <img src="https://cdn.jsdelivr.net/gh/devicons/devicon/icons/docker/docker-original.svg" height="40" alt="docker" />
</div>

* **OpenMetadata 1.6.5** — Open-source data catalog with automated metadata ingestion and lineage visualization.
* **OpenLineage** — Standard API for lineage event collection from Airflow and dbt.

### Data Transformation & Orchestration
<div align="left">
  <img src="https://cdn.jsdelivr.net/gh/devicons/devicon/icons/apacheairflow/apacheairflow-original.svg" height="40" alt="airflow" />
</div>

* **Apache Airflow 2.9.3** — DAG-based workflow orchestrator running dbt models on schedule.
* **dbt-postgres 1.8.2** — SQL-based transformation framework building staging and mart layers.

### Alerting & Monitoring
<div align="left">
  <img src="https://cdn.jsdelivr.net/gh/devicons/devicon/icons/prometheus/prometheus-original.svg" height="40" alt="prometheus" />
</div>

* **Prometheus** — Time-series database scraping Airflow metrics via StatsD Exporter.
* **AlertManager** — Alert routing engine with Slack webhook integration.

### Centralized Logging
<div align="left">
  <img src="https://cdn.jsdelivr.net/gh/devicons/devicon/icons/grafana/grafana-original.svg" height="40" alt="grafana" />
</div>

* **Grafana Loki 2.9.9** — Log aggregation engine optimized for label-based querying (LogQL).
* **Promtail 2.9.9** — Log shipping agent using Docker service discovery.

---

## Directory Structure

```
data-platform-governance/
├── docker-compose.yml              # Root orchestrator (includes all service stacks)
├── .env.example                    # Environment variables template
├── Makefile                        # 25+ operational commands for full lifecycle
├── README.md                       # This file
│
├── dbt/
│   └── fmcg_transformation/        # dbt project for data transformation
│       ├── dbt_project.yml         # Project configuration
│       ├── profiles.yml            # Connection to Project A PostgreSQL (env vars)
│       └── models/
│           ├── staging/
│           │   ├── _sources.yml              # Source definitions + data quality tests
│           │   └── stg_pos_transactions.sql  # Clean, enrich, classify raw data
│           └── marts/
│               ├── mart_sales_by_region.sql       # Regional sales aggregation
│               └── mart_product_performance.sql   # Product ranking & revenue share
│
├── scripts/
│   └── init_postgres_source.sql    # Bootstrap: create table & seed 5,000 records
│
└── services/
    ├── openmetadata/               # Phase 1: Data Catalog
    │   └── docker-compose.yml      # MySQL + Elasticsearch + Server + Ingestion
    │
    ├── airflow/                    # Phase 2: Orchestration & Transformation
    │   ├── docker-compose.yml      # Postgres + Init + Webserver + Scheduler
    │   ├── Dockerfile              # Custom image: Airflow + dbt + OpenLineage
    │   ├── requirements.txt        # Python dependencies
    │   └── dags/
    │       ├── dbt_fmcg_transformation.py  # 7-step dbt pipeline DAG
    │       └── seed_pos_transactions.py     # Bootstrap data seeder DAG
    │
    ├── alerting/                   # Phase 3: SLO Alerting
    │   ├── docker-compose.yml      # StatsD Exporter + Prometheus + AlertManager
    │   └── config/
    │       ├── prometheus.yml      # Scrape targets configuration
    │       ├── alert_rules.yml     # 4 alert rules (DAG fail, SLO, scheduler, task rate)
    │       ├── alertmanager.yml    # Slack routing with severity-based channels
    │       ├── statsd-mapping.yml  # Airflow StatsD → Prometheus metric mapping
    │       └── templates/
    │           └── slack.tmpl      # Slack message formatting template
    │
    └── logging/                    # Phase 4: Centralized Logging
        ├── docker-compose.yml      # Loki + Promtail
        ├── config/
        │   ├── loki-config.yml     # Storage, retention, and schema configuration
        │   └── promtail-config.yml # Docker service discovery & log labeling
        └── grafana/
            ├── dashboards/
            │   └── centralized_logs.json   # 9-panel Grafana dashboard
            └── provisioning/
                ├── dashboards/dashboards.yaml
                └── datasources/loki.yaml   # Loki datasource for Grafana
```

---

## Quick Start Guide

### Prerequisites
* **Docker** and **Docker Compose** (v2+) installed.
* **Project A** (`fmcg-real-time-analytics`) running with the Docker network `fmcg_fmcg-network` active.

### Step 1: Initialize Environment
```bash
# Clone the repository
git clone https://github.com/phatle224/data-platform-governance.git
cd data-platform-governance

# Copy environment template
cp .env.example .env

# (Optional) Update SLACK_WEBHOOK_URL in .env for Slack alerting
```

### Step 2: Bootstrap Source Data
Ensure Project A is running, then seed the PostgreSQL source table:
```bash
# Verify Project A network is available
make check-network

# Create pos_transactions table and insert 5,000 sample records
make init-postgres
```

### Step 3: Launch the Full Stack
```bash
# Start all 14 services (OpenMetadata, Airflow, Alerting, Logging)
make up

# Wait ~2-3 minutes, then verify health
make status
```

### Step 4: Integrate with Grafana
```bash
# Copy Loki datasource + Log dashboard into Project A's Grafana
make provision-grafana
```

### Step 5: Run dbt Transformations
```bash
# Verify dbt can reach Project A's PostgreSQL
make dbt-debug

# Execute transformations: staging → mart
make dbt-run

# Run data quality tests
make dbt-test
```

---

## Service Endpoints

| Service | URL | Credentials | Purpose |
|---|---|---|---|
| **OpenMetadata UI** | [http://localhost:8585](http://localhost:8585) | `admin` / `admin` | Data catalog, lineage graph, metadata search |
| **OM Ingestion** | [http://localhost:8081](http://localhost:8081) | `admin` / `admin` | Metadata ingestion pipeline management |
| **Airflow UI** | [http://localhost:8082](http://localhost:8082) | `admin` / `admin` | DAG monitoring, dbt run triggers |
| **Prometheus** | [http://localhost:9095](http://localhost:9095) | — | Airflow metrics explorer, alert rule status |
| **AlertManager** | [http://localhost:9093](http://localhost:9093) | — | Active alerts, silences, notification status |
| **Loki API** | [http://localhost:3100](http://localhost:3100) | — | Log query API (used by Grafana) |
| **Grafana** | [http://localhost:3000](http://localhost:3000) | `admin` / `admin123` | Centralized log dashboard (via Project A) |

---

## dbt Lineage Model

The transformation pipeline reads raw POS transaction data from Project A's PostgreSQL and produces analytics-ready tables:

| Model | Type | Description |
|---|---|---|
| `stg_pos_transactions` | View | Cleans raw data, maps region codes to Vietnamese names, extracts time dimensions, classifies transaction value tiers |
| `mart_sales_by_region` | Table | Daily sales aggregation by region, product category, and store type with revenue percentages |
| `mart_product_performance` | Table | Product-level metrics with intra-category revenue ranking and daily revenue share percentage |

### Data Quality Tests
* **Source tests**: `transaction_id` uniqueness & not-null, `category` accepted values, `region` accepted values
* **Staging tests**: `transaction_id` uniqueness, `total_amount` not-null
* **Mart tests**: `transaction_date` not-null, `total_revenue` not-null

---

## Alert Rules & SLO Definitions

| Alert Name | Severity | Condition | Action |
|---|---|---|---|
| `AirflowDagFailed` | 🔴 Critical | Any DAG run fails within 5 minutes | Slack notification (immediate) |
| `PipelineSLOViolated` | 🟡 Warning | Average DAG runtime exceeds 300 seconds | Slack notification (1 min buffer) |
| `AirflowSchedulerDown` | 🔴 Critical | StatsD exporter unreachable for 2 minutes | Slack notification (immediate) |
| `HighDagTaskFailureRate` | 🟡 Warning | Task failure rate exceeds 20% over 15 minutes | Slack notification (5 min buffer) |

---

## Troubleshooting

* **Error: `fmcg_fmcg-network NOT found`**
  * *Cause*: Project A is not running.
  * *Fix*: Start Project A first: `cd D:\project\fmcg-real-time-analytics && make up`

* **Error: OpenMetadata cannot connect to `fmcg-postgres`**
  * *Cause*: The OpenMetadata container is not on the shared network.
  * *Fix*: Verify the `fmcg-network` section in `services/openmetadata/docker-compose.yml` has `external: true`.

* **Error: Airflow DAG import errors**
  * *Cause*: dbt packages not installed in the custom Airflow image.
  * *Fix*: Rebuild the image: `docker compose build airflow-webserver airflow-scheduler`

* **Error: Promtail cannot discover containers**
  * *Cause*: Docker socket not mounted correctly (common on Windows).
  * *Fix*: Ensure Docker Desktop is running and the volume mount `//var/run/docker.sock:/var/run/docker.sock:ro` is accessible.

* **Error: AlertManager not sending Slack notifications**
  * *Cause*: `SLACK_WEBHOOK_URL` in `.env` is still the placeholder value.
  * *Fix*: Create a Slack Incoming Webhook at [api.slack.com/messaging/webhooks](https://api.slack.com/messaging/webhooks) and update the URL in `.env`.

---

<div>
  <img style="width: 100%" src="https://capsule-render.vercel.app/api?type=waving&height=120&section=footer&reversal=true&text=Govern%20it%20clean%20%E2%80%A2%20Observe%20it%20reliably&fontSize=22&fontColor=ffffff&fontAlign=50&fontAlignY=50&rotate=0&stroke=-&animation=twinkling&textBg=false&color=gradient" />
</div>
