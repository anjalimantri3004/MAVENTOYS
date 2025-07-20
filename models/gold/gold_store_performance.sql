{{ config(
    materialized='table',
    schema='gold'
) }}

with sales_data as (

    select
        s.store_id,
        s.product_id,
        s.sale_date,
        s.units,
        p.product_price,
        p.product_cost,
        (s.units * p.price) as sales_amount,
        (s.units * p.cost) as cost_amount
    from {{ ref('int_sales_cleaned') }} s
    left join {{ ref('int_products_cleaned') }} p
        on s.product_id = p.product_id

),

aggregated as (

    select
        store_id,
        product_id,
        sum(sales_amount) as total_sales_amount,
        sum(cost_amount) as total_cost_amount,
        sum(units) as total_units_sold,
        min(sale_date) as first_sale_date,
        max(sale_date) as last_sale_date
    from sales_data
    group by store_id, product_id

),

store_info as (

    select
        store_id,
        store_name,
        city,
        region
    from {{ ref('int_stores_cleaned') }}
)

select
    a.store_id,
    si.store_name,
    si.city,
    si.region,
    a.product_id,
    a.total_units_sold,
    a.total_sales_amount,
    a.total_cost_amount,
    a.first_sale_date,
    a.last_sale_date
from aggregated a
left join store_info si
    on a.store_id = si.store_id
order by a.product_id desc