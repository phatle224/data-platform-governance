.PHONY: help up down restart logs ps \
       openmetadata-up airflow-up \
       init-postgres dbt-run dbt-test

# ── Default ────────────────────────────────────────────────────────────────────
help:
	@echo ""
	@echo "  Data Platform Governance & Observability Stack"
	@echo "  ═══════════════════════════════════════════════"
	@echo ""
	@echo "  Prerequisites:"
	@echo "    Project A (fmcg-real-time-analytics) must be running first!"
	@echo "    The 'fmcg_fmcg-network' Docker network must exist."
	@echo ""
	@echo "  Full Stack Commands:"
	@echo "    make up              Start all governance services"
	@echo "    make down            Stop all governance services"
	@echo "    make restart         Restart all services"
	@echo "    make logs            Tail all logs"
	@echo "    make ps              Show running containers"
	@echo ""
	@echo "  Individual Stacks:"
	@echo "    make openmetadata-up Start OpenMetadata stack only"
	@echo "    make airflow-up      Start Airflow + dbt stack only"
	@echo ""
	@echo "  Bootstrap Commands:"
	@echo "    make init-postgres   Create & seed pos_transactions in Project A PG"
	@echo "    make check-network   Verify Project A network is available"
	@echo ""
	@echo "  dbt Commands:"
	@echo "    make dbt-run         Run dbt models inside Airflow container"
	@echo "    make dbt-test        Run dbt tests inside Airflow container"
	@echo "    make dbt-docs        Generate dbt documentation"
	@echo ""
	@echo "  URLs:"
	@echo "    OpenMetadata UI:     http://localhost:8585  (admin/admin)"
	@echo "    OM Ingestion:        http://localhost:8081  (admin/admin)"
	@echo "    Airflow UI:          http://localhost:8082  (admin/admin)"
	@echo ""

# ── Copy env ───────────────────────────────────────────────────────────────────
env:
	@if not exist .env (copy .env.example .env && echo ".env created") else (echo ".env already exists")

# ── Network Check ──────────────────────────────────────────────────────────────
check-network:
	@echo "Checking if Project A network exists..."
	@docker network inspect fmcg_fmcg-network >nul 2>&1 && echo "✓ fmcg_fmcg-network found" || (echo "✗ fmcg_fmcg-network NOT found. Start Project A first!" && exit 1)

# ── Full Stack ─────────────────────────────────────────────────────────────────
up: env check-network
	docker compose up -d --build

down:
	docker compose down

restart:
	docker compose restart

logs:
	docker compose logs -f --tail=100

ps:
	docker compose ps

# ── Individual Stacks ──────────────────────────────────────────────────────────
openmetadata-up: check-network
	docker compose -f services/openmetadata/docker-compose.yml up -d

openmetadata-down:
	docker compose -f services/openmetadata/docker-compose.yml down

airflow-up: check-network
	docker compose -f services/airflow/docker-compose.yml up -d --build

airflow-down:
	docker compose -f services/airflow/docker-compose.yml down

# ── Bootstrap: Initialize PostgreSQL source ────────────────────────────────────
init-postgres:
	@echo "Creating pos_transactions table and seeding data in Project A PostgreSQL..."
	docker exec -i fmcg-postgres psql -U postgres -d fmcg < scripts/init_postgres_source.sql
	@echo "Done! Check with: docker exec fmcg-postgres psql -U postgres -d fmcg -c 'SELECT count(*) FROM pos_transactions'"

verify-postgres:
	docker exec fmcg-postgres psql -U postgres -d fmcg -c "SELECT count(*) as row_count FROM pos_transactions; SELECT region, count(*) as tx_count, sum(total_amount) as revenue FROM pos_transactions GROUP BY region ORDER BY revenue DESC;"

# ── dbt Commands (run inside Airflow container) ───────────────────────────────
dbt-run:
	docker exec -w /opt/airflow/dbt/fmcg_transformation gov-airflow-scheduler \
		dbt run --profiles-dir /opt/airflow/dbt/fmcg_transformation

dbt-test:
	docker exec -w /opt/airflow/dbt/fmcg_transformation gov-airflow-scheduler \
		dbt test --profiles-dir /opt/airflow/dbt/fmcg_transformation

dbt-docs:
	docker exec -w /opt/airflow/dbt/fmcg_transformation gov-airflow-scheduler \
		dbt docs generate --profiles-dir /opt/airflow/dbt/fmcg_transformation

dbt-debug:
	docker exec -w /opt/airflow/dbt/fmcg_transformation gov-airflow-scheduler \
		dbt debug --profiles-dir /opt/airflow/dbt/fmcg_transformation

# ── Status & Health ────────────────────────────────────────────────────────────
status:
	@echo "=== OpenMetadata ==="
	@curl -s http://localhost:8586/healthcheck 2>nul && echo " ✓ OpenMetadata Server healthy" || echo " ✗ OpenMetadata Server not reachable"
	@echo ""
	@echo "=== Airflow ==="
	@curl -s http://localhost:8082/health 2>nul && echo " ✓ Airflow Webserver healthy" || echo " ✗ Airflow Webserver not reachable"
	@echo ""
	@echo "=== Elasticsearch ==="
	@curl -s http://localhost:19200/_cluster/health?pretty 2>nul || echo " ✗ Elasticsearch not reachable"
