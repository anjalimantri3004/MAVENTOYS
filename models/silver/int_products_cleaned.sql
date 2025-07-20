{{ config(materialized='table', schema='public') }}

with source as (
    select * from {{ ref('stg_products') }}
),

cleaned as (
    select
        product_id,
        initcap(product_name) as product_name,
        upper(product_category) as category,
        round(product_price, 2) as price,
        round(product_cost, 2) as cost,
        current_timestamp() as updated_at
    from source
    where product_id is not null and product_name is not null
)

select * from cleaned