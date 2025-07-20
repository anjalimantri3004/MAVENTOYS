{{ config(
    materialized='table',
    schema='public'
) }}

with source_data as (

    select
        cast("Store_ID" as varchar) as store_id,
        cast("Store_Name" as varchar) as store_name,
        cast("City" as varchar) as city,
        cast("State" as varchar) as state,
        cast("Region" as varchar) as region

    from {{ ref('stg_stores') }}

)

select * from source_data