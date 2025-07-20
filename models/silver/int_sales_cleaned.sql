{{ config(
    materialized='table',
    schema='public'
) }}

with cleaned as (

    select
        sale_id,
        sale_date,
        store_id,
        product_id,
        units,
        ingestion_timestamp
    from {{ ref('stg_sales') }}
    where sale_id is not null
      and store_id is not null
      and product_id is not null
      and sale_date is not null
)

select * from cleaned