{{ config(
    materialized='table',
    schema='public'
) }}

with source_data as (

    select
        try_cast('DATE' as date) as calendar_date,
        cast('Day' as varchar) as day_name,
        cast('Month' as varchar) as month_name,
        try_cast('Month_Num' as integer) as month_number,
        try_cast('Year' as integer) as year,
        cast('Quarter' as varchar) as quarter,
        cast('Holiday' as boolean) as is_holiday

    from {{ ref('stg_calender') }}

)

select * from source_data