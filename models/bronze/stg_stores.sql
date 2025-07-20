{{ config(
    materialized='table',
    schema='public'
) }}

with source_data as (

    select
        cast("STORE_ID" as varchar) as store_id,
        cast("STORE_NAME" as varchar) as store_name,
        cast("STORE_CITY" as varchar) as store_city,
        cast("STORE_LOCATION" as varchar) as store_location,
        try_cast("STORE_OPEN_DATE" as date) as store_open_date,
        cast("Capacity" as integer) as capacity,
        current_timestamp() as ingestion_timestamp

    from {{ source('public', 'stores') }}

)

select * from source_data