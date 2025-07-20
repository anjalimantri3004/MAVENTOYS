{{ config(
    materialized='table',
    schema='public'
) }}

with bronze as (
    select * from {{ ref('stg_calender') }}  -- 'stg_calendar' is your bronze table name
),

cleaned as (
    select
        calendar_date,
        day,
        month,
        year,
        day_of_week,
        current_timestamp() as last_updated  -- track when cleaned
    from bronze
    where calendar_date is not null
)

select * from cleaned