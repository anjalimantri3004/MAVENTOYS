{{ config(materialized='table',schema='public') }}

with source_data as (
    select
     try_cast("DATE" as date) as calendar_date,
     date_part("DAY", try_cast("DATE" as date)) as day,
     date_part("MONTH", try_cast("DATE" as date)) as month,
     date_part("YEAR", try_cast("DATE" as date)) as year,
     date_part("DOW", try_cast("DATE" as date)) as day_of_week,
     current_timestamp() as ingestion_timestamp
    from{{ source('public','calendar') }}
)
select * from source_data