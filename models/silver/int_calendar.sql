{{ config(
    materialized='table',
    schema='public'
) }}

with source_data as (

    select
        cast("Sale_ID" as varchar) as sale_id,
        try_cast("Date" as date) as sale_date,
        cast("Store_ID" as varchar) as store_id,
        cast("Product_ID" as varchar) as product_id,
        try_cast("Units" as integer) as units,
        current_timestamp() as ingestion_timestamp

    from {{ ref('bronze__sales') }}  -- use the Bronze table ref here

),

cleaned_data as (

    select
        sale_id,
        sale_date,
        store_id,
        product_id,
        units,
        ingestion_timestamp
    from source_data
    where sale_id is not null
        and store_id is not null
        and product_id is not null
        and sale_date is not null

)

select * from cleaned_data