{{ config(
    materialized='table',
    schema='public'
) }}

with source_data as (

    select
        cast("PRODUCT_ID" as varchar) as product_id,
        cast("PRODUCT_NAME" as varchar) as product_name,
        cast("PRODUCT_CATEGORY" as varchar) as product_category,

        -- Remove '$' and cast to numeric (Silver layer can handle invalid formats)
        try_cast(replace("PRODUCT_COST", '$', '') as float) as product_cost,
        try_cast(replace("PRODUCT_PRICE", '$', '') as float) as product_price,

        current_timestamp() as ingestion_timestamp

    from {{ source('public', 'products') }}

)

select * from source_data