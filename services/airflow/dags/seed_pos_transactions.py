"""
══════════════════════════════════════════════════════════════════════════════
Airflow DAG: seed_pos_transactions_to_postgres
Seeds POS transaction data into Project A's PostgreSQL for dbt consumption.
This is needed because the primary data flow is Kafka → ClickHouse.
We create a mirrored table in PostgreSQL for dbt to transform.
══════════════════════════════════════════════════════════════════════════════
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

default_args = {
    "owner": "data-platform-governance",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

# SQL to create the pos_transactions table in PostgreSQL (mirror of ClickHouse)
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS public.pos_transactions (
    transaction_id  VARCHAR(36) PRIMARY KEY,
    pos_id          VARCHAR(10) NOT NULL,
    product_id      VARCHAR(10) NOT NULL,
    product_name    VARCHAR(100) NOT NULL,
    category        VARCHAR(30) NOT NULL,
    quantity        SMALLINT NOT NULL,
    unit_price      NUMERIC(15, 2) NOT NULL,
    total_amount    NUMERIC(15, 2) NOT NULL,
    region          VARCHAR(5) NOT NULL,
    store_type      VARCHAR(20) NOT NULL,
    timestamp       TIMESTAMPTZ NOT NULL,
    insert_time     TIMESTAMPTZ DEFAULT NOW()
);

-- Create indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_pos_tx_region ON public.pos_transactions(region);
CREATE INDEX IF NOT EXISTS idx_pos_tx_category ON public.pos_transactions(category);
CREATE INDEX IF NOT EXISTS idx_pos_tx_timestamp ON public.pos_transactions(timestamp);
CREATE INDEX IF NOT EXISTS idx_pos_tx_product_id ON public.pos_transactions(product_id);
"""

# SQL to generate sample data (simulates what would come from a Kafka → PG sink)
SEED_SAMPLE_DATA_SQL = """
INSERT INTO public.pos_transactions (
    transaction_id, pos_id, product_id, product_name, category,
    quantity, unit_price, total_amount, region, store_type, timestamp
)
SELECT
    gen_random_uuid()::varchar,
    'POS_' || lpad((floor(random() * 1000 + 1))::int::text, 4, '0'),
    'SKU_' || lpad((floor(random() * 12 + 1))::int::text, 3, '0'),
    (ARRAY[
        'Vinamilk Tuoi Tiet Trung 1L', 'Vinamilk Tuoi Thanh Trung 500ml',
        'Vinamilk Organic Tuoi 180ml', 'Vinamilk ADM GOLD 900g',
        'Sua Chua Vinamilk Co Duong 100g', 'Sua Chua Vinamilk Khong Duong 100g',
        'Sua Chua Uong Vinamilk 130ml', 'Sua Chua Uong Probi 130ml',
        'Nuoc Ep Cam Vfresh 1L', 'Nuoc Ep Buoi Vfresh 200ml',
        'Sua Dac Ngoi Sao Phuong Nam 380g', 'Sua Dac Vinamilk Ong Tho 380g'
    ])[floor(random() * 12 + 1)::int],
    (ARRAY['dairy', 'dairy', 'dairy', 'dairy', 'yogurt', 'yogurt',
           'drinking_yogurt', 'drinking_yogurt', 'juice', 'juice',
           'condensed_milk', 'condensed_milk'])[floor(random() * 12 + 1)::int],
    floor(random() * 5 + 1)::int,
    round((random() * 350000 + 7000)::numeric, -3),
    0,  -- will be computed below
    (ARRAY['HN', 'HCM', 'DN', 'CT', 'HP', 'BD'])[
        (ARRAY[0.28, 0.63, 0.75, 0.85, 0.93, 1.0]::float[] @> ARRAY[random()::float])::int + 1
    ],
    (ARRAY['supermarket', 'convenience', 'wet_market', 'mini_mart'])[floor(random() * 4 + 1)::int],
    NOW() - (random() * interval '30 days')
FROM generate_series(1, 5000)
ON CONFLICT (transaction_id) DO NOTHING;

-- Fix total_amount = quantity * unit_price
UPDATE public.pos_transactions
SET total_amount = quantity * unit_price
WHERE total_amount = 0;
"""

with DAG(
    dag_id="seed_pos_transactions_to_postgres",
    default_args=default_args,
    description="Create and seed the pos_transactions table in Project A PostgreSQL. "
                "Run once to bootstrap data for dbt transformations.",
    schedule_interval=None,  # Manual trigger only
    start_date=datetime(2026, 7, 1),
    catchup=False,
    tags=["seed", "bootstrap", "postgres"],
    max_active_runs=1,
) as dag:

    create_table = BashOperator(
        task_id="create_pos_transactions_table",
        bash_command=(
            'PGPASSWORD=$DBT_POSTGRES_PASSWORD psql '
            '-h $DBT_POSTGRES_HOST '
            '-p $DBT_POSTGRES_PORT '
            '-U $DBT_POSTGRES_USER '
            '-d $DBT_POSTGRES_DB '
            '-c "' + CREATE_TABLE_SQL.replace('"', '\\"').replace('\n', ' ') + '"'
        ),
    )

    seed_data = BashOperator(
        task_id="seed_sample_data",
        bash_command=(
            'PGPASSWORD=$DBT_POSTGRES_PASSWORD psql '
            '-h $DBT_POSTGRES_HOST '
            '-p $DBT_POSTGRES_PORT '
            '-U $DBT_POSTGRES_USER '
            '-d $DBT_POSTGRES_DB '
            '-c "' + SEED_SAMPLE_DATA_SQL.replace('"', '\\"').replace('\n', ' ') + '"'
        ),
    )

    create_table >> seed_data
