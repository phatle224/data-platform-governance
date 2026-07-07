# ══════════════════════════════════════════════════════════════════════════════
# Custom Airflow Image with dbt-postgres & OpenLineage
# ══════════════════════════════════════════════════════════════════════════════
FROM apache/airflow:2.9.3-python3.11

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

USER airflow

# Install dbt-postgres and OpenLineage providers
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
