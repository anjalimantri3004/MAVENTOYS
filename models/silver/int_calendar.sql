{ config(materialized='table', schema='public') }}

with source as (
    select * from {{ ref('stg_calendar') }}  -- assuming your bronze model is 'stg_calendar'
),

cleaned as (
    select
        try_cast(date as date) as calendar_date,
        cast(day as integer) as day,
        cast(month as integer) as month,
        cast(year as integer) as year,
        cast(day_of_week as varchar) as weekday,
        case
            when lower(is_weekend) in ('yes', 'y', 'true', '1') then true
            else false
        end as is_weekend
    from source
    where date is not null
)

select * from cleaned