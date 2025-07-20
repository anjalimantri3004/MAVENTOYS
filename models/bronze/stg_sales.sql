--SELECT * FROM MAVENTOYS.PUBLIC.SALES
{{ config(
    materialized='table',
    schema='public'
) }}

with source_data as (

    select
        -- Raw columns with light cleanup
        cast(order_id as varchar) as order_id,
        cast(customer_id as varchar) as customer_id,
        try_cast(order_date as date) as order_date,
        cast(product_id as varchar) as product_id,
        try_cast(quantity as integer) as quantity,
        try_cast(price as float) as price,
        try_cast(total_amount as float) as total_amount,

        -- Add ingestion timestamp
        current_timestamp() as ingestion_timestamp

    from {{ source('public', 'staging_sales') }}

)

select * from source_data