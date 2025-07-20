{{ config(
    materialized='table',
    schema='public'
) }}

with source_data as (

    select
        cast("Inventory_ID" as varchar) as inventory_id,
        cast("Product_ID" as varchar) as product_id,
        cast("Store_ID" as varchar) as store_id,
        try_cast("Stock_Level" as integer) as stock_level,
        try_cast("Last_Updated" as timestamp) as last_updated

    from {{ ref('stg_inventory') }}

)

select * from source_data