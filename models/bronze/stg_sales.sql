{{ config(
    materialized='table',
    schema='public'
) }}

with source_data as (

    select
        cast("SALE_ID" as varchar) as sale_id,
        try_cast("DATE" as date) as sale_date,
        cast("STORE_ID" as varchar) as store_id,
        cast("PRODUCT_ID" as varchar) as product_id,
        try_cast("UNITS" as integer) as units,
        current_timestamp() as ingestion_timestamp

    from {{ source('public','sales') }}

)

select * from source_data