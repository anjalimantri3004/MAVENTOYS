{{ config(
    materialized='table',
    schema='public'
) }}

with sales as (
    select *
    from {{ ref('silver_sales_cleaned') }}
),

products as (
    select *
    from {{ ref('silver_products_cleaned') }}
),

stores as (
    select *
    from {{ ref('silver_stores_cleaned') }}
),

calendar as (
    select *
    from {{ ref('silver_calendar') }}
)

select
    cal.calendar_date,
    prod.product_id,
    prod.product_name,
    str.store_id,
    str.store_name,
    sum(sal.sales_amount) as total_sales_amount,
    sum(sal.quantity_sold) as total_quantity_sold
from sales sal
join products prod on sal.product_id = prod.product_id
join stores str on sal.store_id = str.store_id
join calendar cal on sal.date = cal.calendar_date
group by
    cal.calendar_date,
    prod.product_id,
    prod.product_name,
    str.store_id,
    str.store_name