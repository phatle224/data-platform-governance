"""
══════════════════════════════════════════════════════════════════════════════
Airflow DAG: dbt_fmcg_transformation
Orchestrates dbt models for FMCG data transformation pipeline.
Emits OpenLineage events to OpenMetadata for automatic lineage tracking.
══════════════════════════════════════════════════════════════════════════════
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

# ── DAG Configuration ─────────────────────────────────────────────────────────
DBT_PROJECT_DIR = "/opt/airflow/dbt/fmcg_transformation"
DBT_PROFILES_DIR = DBT_PROJECT_DIR  # profiles.yml is inside the project dir

default_args = {
    "owner": "data-platform-governance",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="dbt_fmcg_transformation",
    default_args=default_args,
    description="Run dbt models to transform FMCG POS data (staging → mart). "
                "Emits OpenLineage events for automatic lineage in OpenMetadata.",
    schedule_interval="0 6 * * *",  # Daily at 06:00 UTC
    start_date=datetime(2026, 7, 1),
    catchup=False,
    tags=["dbt", "fmcg", "transformation", "openlineage"],
    max_active_runs=1,
) as dag:

    # ── Task 1: dbt deps ─────────────────────────────────────────────────────
    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt deps --profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    # ── Task 2: dbt seed (load reference data if any) ────────────────────────
    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt seed --profiles-dir {DBT_PROFILES_DIR} --full-refresh"
        ),
    )

    # ── Task 3: dbt run (staging models) ─────────────────────────────────────
    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt run --profiles-dir {DBT_PROFILES_DIR} "
            f"--select tag:staging"
        ),
    )

    # ── Task 4: dbt test (staging) ───────────────────────────────────────────
    dbt_test_staging = BashOperator(
        task_id="dbt_test_staging",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt test --profiles-dir {DBT_PROFILES_DIR} "
            f"--select tag:staging"
        ),
    )

    # ── Task 5: dbt run (mart models) ────────────────────────────────────────
    dbt_run_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt run --profiles-dir {DBT_PROFILES_DIR} "
            f"--select tag:mart"
        ),
    )

    # ── Task 6: dbt test (marts) ─────────────────────────────────────────────
    dbt_test_marts = BashOperator(
        task_id="dbt_test_marts",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt test --profiles-dir {DBT_PROFILES_DIR} "
            f"--select tag:mart"
        ),
    )

    # ── Task 7: Generate dbt docs (for reference) ────────────────────────────
    dbt_docs_generate = BashOperator(
        task_id="dbt_docs_generate",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt docs generate --profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    # ── Task Dependencies ─────────────────────────────────────────────────────
    # deps → seed → staging run → staging test → marts run → marts test → docs
    dbt_deps >> dbt_seed >> dbt_run_staging >> dbt_test_staging >> dbt_run_marts >> dbt_test_marts >> dbt_docs_generate
