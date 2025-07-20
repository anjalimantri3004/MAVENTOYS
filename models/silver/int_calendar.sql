{{ config(materialized='table', schema='public') }}

with source as (
    select * from {{ ref('stg_calendar') }}  -- assuming your bronze model is named 'stg_calendar'
),

cleaned as (
    select
        try_cast("calendar_date" as date) as calendar_date,
        try_cast("day" as integer) as day,
        try_cast("month" as integer) as month,
        try_cast("year" as integer) as year,
        cast("day_of_week" as varchar) as day_of_week,
        current_timestamp() as last_updated
    from source
    where "date" is not null
)

select * from cleaned