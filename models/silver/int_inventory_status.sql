{{ config(materialized='table', schema='public') }}

with source as (
    select * from {{ ref('stg_inventory') }}
),

cleaned as (
    select
        inventory_id,
        product_id,
        store_id,
        coalesce(stock_level, 0) as stock_level,
        coalesce(reorder_level, 0) as reorder_level,
        last_updated,
        case
            when stock_level <= reorder_level then 'LOW STOCK'
            else 'IN STOCK'
        end as inventory_status,
        current_timestamp() as updated_at
    from source
    where inventory_id is not null and product_id is not null and store_id is not null
)

select * from cleaned