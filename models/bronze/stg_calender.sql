{{ config(
    materialized='table',
    schema='public'
) }}

with source_data as (

    select
        try_cast("DATE" as date) as calendar_date,
        date_part('day', try_cast("DATE" as date)) as day,
        date_part('month', try_cast("DATE" as date)) as month,
        date_part('year', try_cast("DATE" as date)) as year,
        date_part('dow', try_cast("DATE" as date)) as day_of_week,  -- 0=Sunday in Snowflake
        --to_char(try_cast("Date" as date), 'Day') as day_name
        --current_timestamp() as ingestion_timestamp

    from {{ source('public', 'calendar') }}

)

select * from source_data