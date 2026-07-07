-- ══════════════════════════════════════════════════════════════════════════════
-- Staging: stg_pos_transactions
-- Cleans and standardizes raw POS transaction data from Project A
-- ══════════════════════════════════════════════════════════════════════════════

{{
    config(
        materialized='view',
        tags=['staging', 'daily']
    )
}}

with source as (
    select * from {{ source('raw_fmcg', 'pos_transactions') }}
),

cleaned as (
    select
        -- ── Primary Key ──────────────────────────────────────────────────────
        transaction_id,

        -- ── Terminal Info ─────────────────────────────────────────────────────
        pos_id,

        -- ── Product Dimensions ───────────────────────────────────────────────
        product_id,
        product_name,
        category                    as product_category,

        -- ── Measures ─────────────────────────────────────────────────────────
        quantity,
        unit_price,
        total_amount,

        -- ── Location Dimensions ──────────────────────────────────────────────
        region,
        case region
            when 'HN'  then 'Hà Nội'
            when 'HCM' then 'TP. Hồ Chí Minh'
            when 'DN'  then 'Đà Nẵng'
            when 'CT'  then 'Cần Thơ'
            when 'HP'  then 'Hải Phòng'
            when 'BD'  then 'Bình Dương'
            else 'Unknown'
        end                         as region_name,

        -- ── Store Dimensions ─────────────────────────────────────────────────
        store_type,

        -- ── Time Dimensions ──────────────────────────────────────────────────
        timestamp                   as transaction_at,
        timestamp::date             as transaction_date,
        extract(hour from timestamp) as transaction_hour,
        extract(dow from timestamp)  as day_of_week,

        -- ── Derived Metrics ──────────────────────────────────────────────────
        case
            when total_amount >= 500000 then 'high_value'
            when total_amount >= 100000 then 'medium_value'
            else 'low_value'
        end                         as transaction_tier

    from source
    where
        -- Data quality filter: exclude invalid records
        transaction_id is not null
        and total_amount > 0
        and quantity > 0
)

select * from cleaned
