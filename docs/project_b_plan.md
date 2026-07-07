# 🟢 Project B — Kế Hoạch Triển Khai Chi Tiết (Tích hợp với Project A)

## Data Platform Governance & Observability Stack

> **Mục tiêu:** Hoàn thành trước 20/07/2026 (còn ~13 ngày)
> **Liên kết Project A:** Tích hợp trực tiếp với hệ thống đang chạy của Project A (`D:\project\fmcg-real-time-analytics`) bằng cách sử dụng chung Docker Network và các database instances.
> **Từ khóa CV đạt được:** `OpenMetadata` · `OpenLineage` · `Airflow` · `dbt` · `AlertManager` · `Grafana Loki` · `Docker`

---

## 🔗 Mối Quan Hệ Giữa Project A và Project B

Project B đóng vai trò là **tầng quản trị (Governance)** và **giám sát (Observability)** cho toàn bộ tài nguyên dữ liệu được tạo ra ở Project A. 

```
┌────────────────────────────────────────────────────────────────────────┐
│  PROJECT A (Hạ Tầng Dữ Liệu - FMCG Analytics Platform)                 │
│  - fmcg-postgres (15433 -> 5432)  - fmcg-clickhouse (8123)             │
│  - fmcg-trino (8060 -> 8080)      - fmcg-kafka (19092 -> 29092)        │
│  - Network: fmcg_fmcg-network                                          │
└───────────────────────────────────▲────────────────────────────────────┘
                                    │
                                    │ (Connect & Observe)
                                    │
┌───────────────────────────────────▼────────────────────────────────────┐
│  PROJECT B (Quản Trị & Giám Sát - Governance & Observability)          │
│  - OpenMetadata: Catalog & Lineage cho Postgres, ClickHouse, Trino     │
│  - Airflow & dbt: Chạy transformation models và xuất OpenLineage       │
│  - Loki & Promtail: Gom log từ các container của Project A & B         │
│  - Prometheus & AlertManager: Giám sát Airflow DAGs & Alert Slack      │
└────────────────────────────────────────────────────────────────────────┘
```

### Project B tận dụng những gì từ Project A?
1. **Docker Network (`fmcg_fmcg-network`):** Project B sẽ kết nối vào mạng này để gọi trực tiếp các service của Project A bằng container name (ví dụ: `fmcg-clickhouse:8123`, `fmcg-trino:8080`, `fmcg-postgres:5432`).
2. **Metadata Source:** OpenMetadata kết nối trực tiếp vào các database của Project A để quét (ingest) thông tin bảng biểu, kiểu dữ liệu (schema) và mô tả.
3. **Data Pipeline:** Chúng ta sẽ viết các mô hình **dbt** biến đổi dữ liệu thô (raw transaction) trong PostgreSQL của Project A thành các bảng tổng hợp (mart) và chạy chúng qua **Airflow** của Project B. Việc này giúp sinh ra các event **OpenLineage** thực tế truyền về OpenMetadata.
4. **Log & Metrics Sources:** Promtail của Project B sẽ đọc Docker logs của các container từ Project A (`fmcg-clickhouse`, `fmcg-kafka`, `fmcg-trino`) để đẩy về Loki.

---

## 🏗️ Kiến Trúc Các Cấu Phần Trong Project B

```
┌─────────────────────── DATA LINEAGE & CATALOG ───────────────────────┐
│                                                                        │
│  [Airflow DAGs] ──► [OpenLineage HTTP Listener] ──┐                  │
│  (Chạy trong B)                                   ├──► [OpenMetadata] │
│  [dbt Models] ─────► [dbt-openlineage adapter] ──┘    (Catalog UI +  │
│  (Chạy trong B)                                         Lineage Graph) │
│                                                                        │
│  [CH / Postgres / Trino] (Project A) ──► [OpenMetadata Connector]     │
└────────────────────────────────────────────────────────────────────────┘

┌─────────────────────── ALERTING & SLO ───────────────────────────────┐
│                                                                        │
│  [Airflow (B)] ──► [StatsD Exporter] ──► [Prometheus (A/B)] ──► [Grafana]│
│                                                   │                    │
│                                             [AlertManager] ──► [Slack] │
│                                             (SLO: DAG finish < 06:00)  │
└────────────────────────────────────────────────────────────────────────┘

┌─────────────────────── CENTRALIZED LOGGING ──────────────────────────┐
│                                                                        │
│  [Airflow Logs (B)]   ──┐                                              │
│  [ClickHouse Logs (A)] ─┼──► [Promtail (B)] ──► [Loki (B)] ──► [Grafana]│
│  [Kafka Logs (A)] ────┘                         (LogQL query panels)   │
└────────────────────────────────────────────────────────────────────────┘
```

---

## 📦 Docker Compose & Port Mapping Cho Project B

Để tránh xung đột với Project A, Project B sẽ map các port như sau:

| Service | Host Port (Project B) | Container Port | Ghi chú |
|---|---|---|---|
| `openmetadata-server` | **8585** | 8585 | Giao diện quản trị dữ liệu |
| `elasticsearch` | **9200** | 9200 | Search engine của OpenMetadata |
| `airflow-webserver` | **8081** | 8080 | **Tránh trùng với Kafka UI (8080) và Trino (8080) của Project A** |
| `loki` | **3100** | 3100 | Lưu trữ log tập trung |
| `promtail` | - | - | Đọc log file từ host và đẩy về Loki |
| `alertmanager` | **9093** | 9093 | Tiếp nhận cảnh báo từ Prometheus và đẩy sang Slack |

---

## 🗓️ Lộ Trình Triển Khai Chi Tiết Từng Ngày

### Phase 1: Docker Integration & OpenMetadata (Ngày 1–3: 08–10/07)

**Mục tiêu:** Dựng OpenMetadata kết nối vào mạng `fmcg_fmcg-network` để scan metadata từ PostgreSQL, ClickHouse, và Trino của Project A.

* **Ngày 1 (08/07): Dựng OpenMetadata & Cấu hình Network**
  - [ ] Khởi tạo thư mục dự án `D:\project\data-platform-governance-stack`.
  - [ ] Viết `docker-compose.yml` định nghĩa OpenMetadata, MySQL (metadata DB) và Elasticsearch.
  - [ ] Cấu hình phần `networks` trong compose để kết nối với external network của Project A:
    ```yaml
    networks:
      fmcg-network:
        name: fmcg_fmcg-network
        external: true
    ```
  - [ ] Start hệ thống, kiểm tra OpenMetadata hoạt động tại `http://localhost:8585`.

* **Ngày 2 (09/07): Ingest Metadata của ClickHouse & PostgreSQL**
  - [ ] Trên OpenMetadata UI, tạo Service Connection cho **PostgreSQL**:
    - Host: `fmcg-postgres` (sử dụng container name trong network chung)
    - Port: `5432`
    - DB: `fmcg`
  - [ ] Tạo Service Connection cho **ClickHouse**:
    - Host: `fmcg-clickhouse`
    - Port: `8123`
  - [ ] Cấu hình và chạy Metadata Ingestion Schedule. Xác nhận các table của Project A (`pos_transactions`, `pos_hourly_mv`, v.v.) hiển thị đầy đủ cấu trúc trên OpenMetadata UI.

* **Ngày 3 (10/07): Ingest Metadata của Trino & Tạo Data Quality Test**
  - [ ] Tạo Service Connection cho **Trino**:
    - Host: `fmcg-trino`
    - Port: `8080` (container port)
  - [ ] Thiết lập 2 bài test chất lượng dữ liệu (Data Quality Test) trực tiếp trên OpenMetadata cho bảng `pos_transactions` (ví dụ: cột `total_amount` không được âm, cột `transaction_id` không được null).

---

### Phase 2: Airflow & dbt Transformation with OpenLineage (Ngày 4–6: 11–13/07)

**Mục tiêu:** Viết pipeline dbt chuẩn hóa dữ liệu, chạy bằng Airflow, và tự động vẽ Lineage Graph từ raw table → staging → mart.

* **Ngày 4 (11/07): Thiết lập Airflow & dbt trong Project B**
  - [ ] Thêm Airflow (Webserver, Scheduler, Postgres backend) vào `docker-compose.yml` của Project B.
  - [ ] Khởi tạo một dự án dbt đơn giản `fmcg_transformation` bên trong repo Project B.
  - [ ] Cấu hình `profiles.yml` của dbt trỏ đến PostgreSQL của Project A (`fmcg-postgres:5432`).
  - [ ] Viết các dbt models cơ bản:
    - `stg_pos_transactions.sql` (đọc từ raw table của Project A)
    - `mart_sales_by_region.sql` (aggregate doanh thu theo vùng miền)

* **Ngày 5 (12/07): Tích hợp OpenLineage thu thập metadata**
  - [ ] Cài đặt package `apache-airflow-providers-openlineage` và `dbt-openlineage` trong môi trường Airflow.
  - [ ] Cấu hình endpoint trong Airflow để chuyển tiếp event lineage đến OpenMetadata:
    - OpenLineage URL: `http://openmetadata-server:8585/api/v1/lineage/openlineage`
  - [ ] Viết 1 Airflow DAG chạy `dbt run` định kỳ.

* **Ngày 6 (13/07): Chạy và Xác Thực Lineage Graph**
  - [ ] Trigger Airflow DAG chạy thử dbt model.
  - [ ] Kiểm tra OpenMetadata UI → chọn bảng `mart_sales_by_region` → tab **Lineage**.
  - [ ] Xác nhận lineage graph tự động hiển thị luồng dữ liệu đi từ raw PostgreSQL qua dbt run đến bảng đích một cách trực quan. Chụp ảnh màn hình làm tư liệu.

---

### Phase 3: SLO Alerting & Slack Integration (Ngày 7–8: 14–15/07)

**Mục tiêu:** Thiết lập Prometheus và AlertManager để phát hiện lỗi pipeline hoặc vi phạm cam kết chất lượng dữ liệu (SLO) và thông báo qua Slack.

* **Ngày 7 (14/07): Thu thập metrics của Airflow bằng Prometheus**
  - [ ] Thêm AlertManager và một instance Prometheus độc lập (hoặc tích hợp vào Prometheus có sẵn của Project A) vào Project B compose.
  - [ ] Bật StatsD metrics trong Airflow và cấu hình `statsd_exporter` để Prometheus scrape được thông số DAGs (thời gian chạy, trạng thái success/fail).

* **Ngày 8 (15/07): Định nghĩa Alert Rules & Slack Webhook**
  - [ ] Tạo file `alert_rules.yml` với các rules:
    - `AirflowDagFailed`: Cảnh báo khi có DAG bị fail.
    - `PipelineSLOViolated`: Cảnh báo nếu DAG chạy quá 5 phút (giả lập SLO bị vi phạm).
  - [ ] Tạo Slack channel cá nhân, lấy Webhook URL và cấu hình AlertManager gửi alert trực tiếp vào Slack khi trigger rule. Test thử bằng cách làm lỗi DAG thủ công.

---

### Phase 4: Centralized Logging với Loki & Promtail (Ngày 9–10: 16–17/07)

**Mục tiêu:** Thu thập logs tập trung từ cả 2 Project (Kafka, ClickHouse của Project A và Airflow của Project B) về một nơi duy nhất để dễ debug.

* **Ngày 9 (16/07): Cấu hình Loki & Promtail đọc Docker Logs**
  - [ ] Thêm Loki và Promtail vào compose file của Project B.
  - [ ] Cấu hình `promtail.yml` mount trực tiếp thư mục log của Docker trên host `/var/lib/docker/containers` (hoặc cấu hình Docker logging driver gelf/loki).
  - [ ] Thiết lập Promtail lọc và tag các logs từ các container: `fmcg-clickhouse`, `fmcg-kafka`, và `fmcg-airflow-scheduler`.

* **Ngày 10 (17/07): Tạo Dashboard Logs trên Grafana**
  - [ ] Mở Grafana (có thể dùng Grafana của Project A trên port 3000), add Loki làm data source (`http://loki:3100`).
  - [ ] Thiết kế dashboard hiển thị log lỗi trực tiếp (Error Log stream) của các cấu phần ClickHouse và Airflow sử dụng cú pháp LogQL.

---

### Phase 5: Polish & Documentation (Ngày 11–13: 18–20/07)

**Mục tiêu:** Hoàn thiện source code, viết tài liệu hướng dẫn chạy chi tiết và cập nhật CV.

* **Ngày 11 (18/07): Viết README.md chuyên nghiệp**
  - [ ] Tạo file `README.md` chất lượng cao cho dự án B (`data-platform-governance-stack`).
  - [ ] Vẽ sơ đồ kiến trúc thể hiện rõ cách Project B kết nối và quan trắc Project A.
  - [ ] Viết hướng dẫn setup 3 bước rõ ràng.

* **Ngày 12 (19/07): Test toàn diện (End-to-End Test)**
  - [ ] Chạy luồng giả lập của Project A (bắn event) -> Chạy transformation ở Project B -> Xem schema & lineage cập nhật trong OpenMetadata -> Làm lỗi pipeline -> Nhận alert Slack.
  - [ ] Chụp lại ảnh sắc nét của OpenMetadata Lineage và Slack Alert làm bằng chứng (proof of work).

* **Ngày 13 (20/07): Push GitHub & Hoàn thiện hồ sơ**
  - [ ] Push toàn bộ code lên GitHub repo.
  - [ ] Copy các đoạn mô tả dự án theo chuẩn PSR (Problem-Solution-Result) vào CV của bạn.

---

## 🛠️ Cấu hình Mẫu Cho docker-compose.yml (Mạng kết nối Project A)

Để các bạn hình dung rõ cách Project B nối mạng với Project A, dưới đây là cấu trúc khai báo mạng trong `docker-compose.yml` của Project B:

```yaml
version: '3.8'

services:
  openmetadata-server:
    image: openmetadata/server:1.4.2
    container_name: gov-openmetadata-server
    environment:
      # Cấu hình kết nối DB nội bộ của Project B
      DB_HOST: gov-mysql
      # ...
    ports:
      - "8585:8585"
    networks:
      - fmcg-network # Kết nối vào mạng của Project A
      - gov-internal-network

  # Các services khác như Airflow, Loki, Promtail...
  
networks:
  gov-internal-network:
    driver: bridge
  fmcg-network:
    name: fmcg_fmcg-network # Tên chính xác của network được tạo ra bởi Project A
    external: true
```

---

*Tài liệu được thiết kế và cập nhật dựa trên mã nguồn thực tế của Project A.*
