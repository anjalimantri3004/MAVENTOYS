{{ config(
    materialized='table',
    schema='public'
) }}

with source_data as (

    select
        cast("STORE_ID" as varchar) as store_id,
        cast("PRODUCT_ID" as varchar) as product_id,
        try_cast("STOCK_ON_HAND" as integer) as stock_on_hand,
        try_cast("Stock_Level" as integer) as stock_level,
        try_cast("Reorder_Level" as integer) as reorder_level,
        try_cast("Last_Updated" as timestamp) as last_updated,
        current_timestamp() as ingestion_timestamp
    from {{ source('public', 'inventory') }}

)

select * from source_data