-- ══════════════════════════════════════════════════════════════════════════════
-- PostgreSQL Init Script: Create pos_transactions table
-- This script should be run against Project A's PostgreSQL (fmcg-postgres)
-- to create the table that dbt will use as its source.
-- ══════════════════════════════════════════════════════════════════════════════

-- Create the analytics schema for dbt output
CREATE SCHEMA IF NOT EXISTS analytics;

-- Create pos_transactions table (mirrors ClickHouse structure)
CREATE TABLE IF NOT EXISTS public.pos_transactions (
    transaction_id  VARCHAR(36) PRIMARY KEY,
    pos_id          VARCHAR(10) NOT NULL,
    product_id      VARCHAR(10) NOT NULL,
    product_name    VARCHAR(100) NOT NULL,
    category        VARCHAR(30) NOT NULL,
    quantity        SMALLINT NOT NULL CHECK (quantity > 0),
    unit_price      NUMERIC(15, 2) NOT NULL CHECK (unit_price > 0),
    total_amount    NUMERIC(15, 2) NOT NULL CHECK (total_amount > 0),
    region          VARCHAR(5) NOT NULL,
    store_type      VARCHAR(20) NOT NULL,
    timestamp       TIMESTAMPTZ NOT NULL,
    insert_time     TIMESTAMPTZ DEFAULT NOW()
);

-- Indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_pos_tx_region     ON public.pos_transactions(region);
CREATE INDEX IF NOT EXISTS idx_pos_tx_category   ON public.pos_transactions(category);
CREATE INDEX IF NOT EXISTS idx_pos_tx_timestamp  ON public.pos_transactions(timestamp);
CREATE INDEX IF NOT EXISTS idx_pos_tx_product_id ON public.pos_transactions(product_id);
CREATE INDEX IF NOT EXISTS idx_pos_tx_date       ON public.pos_transactions(DATE(timestamp));

-- ══════════════════════════════════════════════════════════════════════════════
-- Seed: Generate 5,000 sample POS transactions
-- Simulates data that would flow from Kafka → PostgreSQL via Kafka Connect
-- ══════════════════════════════════════════════════════════════════════════════

INSERT INTO public.pos_transactions (
    transaction_id, pos_id, product_id, product_name, category,
    quantity, unit_price, total_amount, region, store_type, timestamp
)
SELECT
    gen_random_uuid()::varchar                                                    AS transaction_id,
    'POS_' || lpad((floor(random() * 1000 + 1))::int::text, 4, '0')             AS pos_id,
    'SKU_' || lpad(product_idx::text, 3, '0')                                    AS product_id,
    product_names[product_idx]                                                    AS product_name,
    product_categories[product_idx]                                               AS category,
    qty                                                                           AS quantity,
    unit_p                                                                        AS unit_price,
    qty * unit_p                                                                  AS total_amount,
    regions[region_idx]                                                           AS region,
    store_types[floor(random() * 4 + 1)::int]                                    AS store_type,
    NOW() - (random() * interval '30 days')                                       AS timestamp
FROM (
    SELECT
        s,
        floor(random() * 12 + 1)::int AS product_idx,
        floor(random() * 5 + 1)::int  AS qty,
        round((random() * 350000 + 7000)::numeric, -3) AS unit_p,
        -- Weighted region selection
        CASE
            WHEN random() < 0.28 THEN 1  -- HN
            WHEN random() < 0.63 THEN 2  -- HCM
            WHEN random() < 0.75 THEN 3  -- DN
            WHEN random() < 0.85 THEN 4  -- CT
            WHEN random() < 0.93 THEN 5  -- HP
            ELSE 6                        -- BD
        END AS region_idx,
        ARRAY[
            'Vinamilk Tuoi Tiet Trung 1L', 'Vinamilk Tuoi Thanh Trung 500ml',
            'Vinamilk Organic Tuoi 180ml', 'Vinamilk ADM GOLD 900g',
            'Sua Chua Vinamilk Co Duong 100g', 'Sua Chua Vinamilk Khong Duong 100g',
            'Sua Chua Uong Vinamilk 130ml', 'Sua Chua Uong Probi 130ml',
            'Nuoc Ep Cam Vfresh 1L', 'Nuoc Ep Buoi Vfresh 200ml',
            'Sua Dac Ngoi Sao Phuong Nam 380g', 'Sua Dac Vinamilk Ong Tho 380g'
        ] AS product_names,
        ARRAY[
            'dairy', 'dairy', 'dairy', 'dairy',
            'yogurt', 'yogurt',
            'drinking_yogurt', 'drinking_yogurt',
            'juice', 'juice',
            'condensed_milk', 'condensed_milk'
        ] AS product_categories,
        ARRAY['HN', 'HCM', 'DN', 'CT', 'HP', 'BD'] AS regions,
        ARRAY['supermarket', 'convenience', 'wet_market', 'mini_mart'] AS store_types
    FROM generate_series(1, 5000) s
) data
ON CONFLICT (transaction_id) DO NOTHING;
