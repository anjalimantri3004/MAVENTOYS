{{ config(materialized='table', schema='public') }}

with source as (
    select * from {{ ref('stg_inventory') }}  -- assuming your bronze model is named 'stg_inventory'
),

cleaned as (
    select
        cast(product_id as varchar) as product_id,
        cast(store_id as varchar) as store_id,
        try_cast(stock_on_hand as integer) as stock_on_hand,
        current_timestamp() as last_updated
    from source
    where product_id is not null
      and store_id is not null
)

select * from cleaned