{{ config(
    materialized='table',
    schema='gold'
) }}

with sales_summary as (

    select
        store_id,
        sum(sales_amount) as total_sales,
        sum(quantity_sold) as total_quantity,
        count(distinct product_id) as distinct_products_sold,
        min(sale_date) as first_sale_date,
        max(sale_date) as last_sale_date
    from {{ ref('int_sales_cleaned') }}
    group by store_id

),

store_details as (
    select
        store_id,
        store_name,
        city,
        region
    from {{ ref('int_stores_cleaned') }}
)

select
    sd.store_id,
    sd.store_name,
    sd.city,
    sd.region,
    ss.total_sales,
    ss.total_quantity,
    ss.distinct_products_sold,
    ss.first_sale_date,
    ss.last_sale_date
from sales_summary ss
join store_details sd
on ss.store_id = sd.store_id