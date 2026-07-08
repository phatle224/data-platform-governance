<div>
  <img style="width: 100%" src="https://capsule-render.vercel.app/api?type=waving&height=120&section=header&reversal=true&text=Data%20Platform%20Governance&fontSize=30&fontColor=ffffff&fontAlign=50&fontAlignY=45&rotate=0&stroke=-&animation=twinkling&desc=OpenMetadata%20%E2%80%A2%20Airflow%20%2B%20dbt%20%E2%80%A2%20AlertManager%20%E2%80%A2%20Grafana%20Loki&descSize=15&descAlign=50&descAlignY=65&textBg=false&color=gradient" />
</div>

<div align="center">
  <a href="README.md">English</a> | <strong>Tiếng Việt</strong>
</div>

<h3 align="center">Quản Trị Dữ Liệu, Theo Dõi Data Lineage, Cảnh Báo SLO & Gom Log Tập Trung cho Nền Tảng Phân Tích Thời Gian Thực FMCG</h3>

<div align="center">
  <img src="https://img.shields.io/badge/Catalog-OpenMetadata-5B4FDB?style=for-the-badge&logoColor=white" alt="openmetadata badge" />
  <img src="https://img.shields.io/badge/Orchestration-Apache%20Airflow-017CEE?style=for-the-badge&logo=apacheairflow&logoColor=white" alt="airflow badge" />
  <img src="https://img.shields.io/badge/Transform-dbt-FF694B?style=for-the-badge&logo=dbt&logoColor=white" alt="dbt badge" />
  <img src="https://img.shields.io/badge/Lineage-OpenLineage-1C1E21?style=for-the-badge&logo=linuxfoundation&logoColor=white" alt="openlineage badge" />
  <img src="https://img.shields.io/badge/Alerting-Prometheus-E6522C?style=for-the-badge&logo=prometheus&logoColor=white" alt="prometheus badge" />
  <img src="https://img.shields.io/badge/Logging-Grafana%20Loki-F2CC0C?style=for-the-badge&logo=grafana&logoColor=black" alt="loki badge" />
  <img src="https://img.shields.io/badge/Infra-Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white" alt="docker badge" />
</div>

---

## Mục Lục

1. [Tổng Quan Dự Án](#tổng-quan-dự-án)
2. [Kiến Trúc Hệ Thống](#kiến-trúc-hệ-thống)
3. [Tính Năng Chính](#tính-năng-chính)
4. [Công Nghệ Sử Dụng](#công-nghệ-sử-dụng)
5. [Hướng Dẫn Khởi Chạy](#hướng-dẫn-khởi-chạy)
6. [Danh Sách Dịch Vụ](#danh-sách-dịch-vụ)
7. [Mô Hình dbt & Lineage](#mô-hình-dbt--lineage)
8. [Quy Tắc Cảnh Báo SLO](#quy-tắc-cảnh-báo-slo)
9. [Xử Lý Sự Cố](#xử-lý-sự-cố)

---

## Tổng Quan Dự Án

Dự án này triển khai **tầng Quản trị (Governance) và Giám sát (Observability)** cho [Nền tảng phân tích thời gian thực FMCG](https://github.com/phatle224/fmcg-real-time-analytics) (Project A). Nó hoạt động như một **repository đồng hành** kết nối vào hạ tầng đang chạy của Project A thông qua Docker network chung để cung cấp:

1. **Data Catalog & Lineage** — OpenMetadata tự động quét schema từ PostgreSQL, ClickHouse, Trino của Project A. OpenLineage thu thập đồ thị lineage từ dbt.
2. **Biến đổi Dữ liệu** — Các mô hình dbt làm sạch và tổng hợp dữ liệu POS thô thành bảng mart phân tích, được điều phối bởi Apache Airflow.
3. **Cảnh báo SLO Pipeline** — Prometheus thu thập metrics của Airflow qua StatsD Exporter và gửi cảnh báo (DAG lỗi, vi phạm SLO) tới Slack qua AlertManager.
4. **Gom Log Tập Trung** — Loki + Promtail thu thập logs Docker container từ cả hai project vào một kho lưu trữ duy nhất, hiển thị trên Grafana bằng LogQL.

### Vấn Đề → Giải Pháp → Kết Quả (PSR)

| Khía cạnh | Mô tả |
|---|---|
| **Vấn đề** | Các tài nguyên dữ liệu thô trong nền tảng phân tích (Project A) thiếu tài liệu hóa, theo dõi lineage, kiểm thử chất lượng và giám sát vận hành tập trung. Debug lỗi production yêu cầu SSH vào từng container riêng lẻ. |
| **Giải pháp** | Triển khai stack quản trị tự động catalog schema (OpenMetadata), theo dõi lineage dữ liệu từ raw → staging → mart (OpenLineage + dbt), giám sát SLO pipeline với cảnh báo ngưỡng (Prometheus + AlertManager → Slack), và gom log tập trung (Loki + Promtail → Grafana). |
| **Kết quả** | Catalog metadata đầy đủ với lineage graph tự động bao phủ 3 nguồn dữ liệu. Pipeline lỗi kích hoạt cảnh báo Slack trong 30 giây. Tìm kiếm log tập trung giảm thời gian debug từ SSH container xuống còn một truy vấn Grafana duy nhất. 14 service được điều phối bằng `make up` không cần cấu hình thủ công. |

---

## Kiến Trúc Hệ Thống

Stack này kết nối vào Project A thông qua Docker network chung `fmcg_fmcg-network`, cho phép giao tiếp container-to-container qua hostname.

```
┌────────────────────────────────────────────────────────────────────────┐
│  PROJECT A (Hạ Tầng Dữ Liệu — Nền Tảng Phân Tích FMCG)               │
│  ┌─────────────┐ ┌──────────────┐ ┌─────────┐ ┌──────────────────┐   │
│  │ fmcg-postgres│ │fmcg-clickhouse│ │fmcg-trino│ │ fmcg-kafka       │   │
│  │  :5432       │ │  :8123        │ │  :8080   │ │  :29092          │   │
│  └──────┬───────┘ └──────┬───────┘ └────┬─────┘ └──────────────────┘   │
│         │                │              │                               │
│         └────────────────┼──────────────┘                               │
│                          │ Docker Network: fmcg_fmcg-network            │
└──────────────────────────┼──────────────────────────────────────────────┘
                           │ (Kết nối & Quan sát)
┌──────────────────────────┼──────────────────────────────────────────────┐
│  PROJECT B (Quản Trị & Giám Sát)                                       │
│                          │                                              │
│  ┌───────────────────────▼────────────────────────────────────────────┐ │
│  │                   DATA CATALOG & LINEAGE                          │ │
│  │  OpenMetadata ◄── Metadata Ingestion (Postgres, CH, Trino)        │ │
│  │       ▲                                                           │ │
│  │       │ OpenLineage Events                                        │ │
│  │  Airflow DAGs ──► dbt run ──► staging ──► mart tables             │ │
│  └───────────────────────────────────────────────────────────────────┘ │
│                                                                        │
│  ┌─────────────────────── CẢNH BÁO SLO PIPELINE ────────────────────┐ │
│  │  Airflow ──► StatsD ──► Prometheus ──► AlertManager ──► Slack     │ │
│  └───────────────────────────────────────────────────────────────────┘ │
│                                                                        │
│  ┌─────────────────────── GOM LOG TẬP TRUNG ────────────────────────┐ │
│  │  Docker Containers ──► Promtail ──► Loki ──► Grafana Dashboard    │ │
│  │  (Project A + B)       (auto-discover)        (LogQL panels)      │ │
│  └───────────────────────────────────────────────────────────────────┘ │
└────────────────────────────────────────────────────────────────────────┘
```

---

## Tính Năng Chính

### 1. Tự Động Catalog Metadata (OpenMetadata)
OpenMetadata kết nối trực tiếp vào các database của Project A thông qua Docker network chung và tự động phát hiện schema bảng, kiểu cột và mô tả. Pipeline ingestion được lập lịch tự động cập nhật catalog.

### 2. Data Lineage End-to-End (OpenLineage + dbt)
Khi Airflow trigger `dbt run`, OpenLineage provider tự động phát event lineage về OpenMetadata. Kết quả là một đồ thị lineage trực quan theo dõi luồng dữ liệu từ bảng nguồn thô qua staging view đến bảng mart tổng hợp.

### 3. Giám Sát SLO Pipeline & Cảnh Báo Slack (Prometheus + AlertManager)
Airflow phát metrics vận hành qua StatsD (thời gian DAG, số task thành công/thất bại, heartbeat scheduler). Prometheus đánh giá các alert rule. Khi DAG lỗi hoặc vượt SLO 5 phút, AlertManager gửi thông báo formatted tới Slack channel trong 30 giây.

### 4. Gom Log Tập Trung (Loki + Promtail)
Promtail tự động phát hiện Docker container từ cả hai project bằng Docker socket, trích xuất log stream và gắn nhãn theo project (`project-a` / `project-b`), tên service và log level. Dashboard Grafana pre-built cung cấp 9 panel giám sát lỗi thời gian thực và phân tích khối lượng log bằng LogQL.

---

## Công Nghệ Sử Dụng

| Thành phần | Công nghệ | Phiên bản |
|---|---|---|
| Data Catalog | OpenMetadata | 1.6.5 |
| Điều phối | Apache Airflow | 2.9.3 |
| Biến đổi dữ liệu | dbt-postgres | 1.8.2 |
| Lineage | OpenLineage | 1.25.0 |
| Metrics | Prometheus + StatsD Exporter | v2.52.0 |
| Cảnh báo | AlertManager | v0.27.0 |
| Log storage | Grafana Loki | 2.9.9 |
| Log collector | Promtail | 2.9.9 |
| Hạ tầng | Docker Compose | v2+ |

---

## Hướng Dẫn Khởi Chạy

### Yêu Cầu
* **Docker** và **Docker Compose** (v2+) đã cài đặt.
* **Project A** (`fmcg-real-time-analytics`) đang chạy với Docker network `fmcg_fmcg-network`.

### Bước 1: Khởi Tạo Môi Trường
```bash
git clone https://github.com/phatle224/data-platform-governance.git
cd data-platform-governance
cp .env.example .env
# (Tùy chọn) Cập nhật SLACK_WEBHOOK_URL trong .env
```

### Bước 2: Tạo Dữ Liệu Nguồn
```bash
make check-network     # Kiểm tra mạng Project A
make init-postgres     # Tạo bảng & seed 5,000 bản ghi
```

### Bước 3: Khởi Chạy Toàn Bộ
```bash
make up                # Khởi chạy 14 service
make status            # Kiểm tra sức khỏe
make provision-grafana # Tích hợp Loki + Dashboard vào Grafana
```

---

## Danh Sách Dịch Vụ

| Dịch vụ | URL | Tài khoản | Mục đích |
|---|---|---|---|
| **OpenMetadata UI** | [http://localhost:8585](http://localhost:8585) | `admin` / `admin` | Data catalog, lineage graph |
| **Airflow UI** | [http://localhost:8082](http://localhost:8082) | `admin` / `admin` | Quản lý DAG, trigger dbt |
| **Prometheus** | [http://localhost:9095](http://localhost:9095) | — | Metrics explorer, alert rules |
| **AlertManager** | [http://localhost:9093](http://localhost:9093) | — | Quản lý cảnh báo |
| **Loki API** | [http://localhost:3100](http://localhost:3100) | — | Log query API |
| **Grafana** | [http://localhost:3000](http://localhost:3000) | `admin` / `admin123` | Dashboard log tập trung |

---

## Mô Hình dbt & Lineage

```
raw_fmcg.pos_transactions (PostgreSQL Project A)
    └──► stg_pos_transactions (view) — Làm sạch, bổ sung tên vùng, phân hạng
         ├──► mart_sales_by_region (table) — Tổng hợp doanh thu theo vùng/ngày
         └──► mart_product_performance (table) — Xếp hạng sản phẩm theo category
```

---

## Quy Tắc Cảnh Báo SLO

| Tên cảnh báo | Mức độ | Điều kiện | Hành động |
|---|---|---|---|
| `AirflowDagFailed` | 🔴 Critical | Bất kỳ DAG nào lỗi trong 5 phút | Thông báo Slack (ngay lập tức) |
| `PipelineSLOViolated` | 🟡 Warning | Thời gian DAG trung bình > 300 giây | Thông báo Slack (đệm 1 phút) |
| `AirflowSchedulerDown` | 🔴 Critical | StatsD exporter không liên lạc được 2 phút | Thông báo Slack (ngay lập tức) |
| `HighDagTaskFailureRate` | 🟡 Warning | Tỷ lệ task lỗi > 20% trong 15 phút | Thông báo Slack (đệm 5 phút) |

---

## Xử Lý Sự Cố

* **Lỗi: `fmcg_fmcg-network NOT found`** — Project A chưa chạy. Chạy `make up` trong thư mục Project A trước.
* **Lỗi: OpenMetadata không kết nối được `fmcg-postgres`** — Kiểm tra cấu hình `external: true` trong phần network.
* **Lỗi: Airflow DAG import errors** — Rebuild image: `docker compose build airflow-webserver airflow-scheduler`
* **Lỗi: Promtail không phát hiện container** — Docker Desktop phải đang chạy, kiểm tra mount Docker socket.
* **Lỗi: AlertManager không gửi Slack** — Cập nhật `SLACK_WEBHOOK_URL` thật trong `.env`.

---

<div>
  <img style="width: 100%" src="https://capsule-render.vercel.app/api?type=waving&height=120&section=footer&reversal=true&text=Quản%20trị%20sạch%20%E2%80%A2%20Giám%20sát%20tin%20cậy&fontSize=22&fontColor=ffffff&fontAlign=50&fontAlignY=50&rotate=0&stroke=-&animation=twinkling&textBg=false&color=gradient" />
</div>
