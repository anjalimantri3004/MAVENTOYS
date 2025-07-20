{{ config(
    materialized='table',
    schema='gold'
) }}

with sales_data as (

    select
        s.product_id,
        s.units,
        p.price,
        p.cost,
        (s.units * p.price) as sales_amount,
        (s.units * p.cost) as cost_amount
    from {{ ref('int_sales_cleaned') }} s
    left join {{ ref('int_products_cleaned') }} p
        on s.product_id = p.product_id

),

aggregated as (

    select
        product_id,
        sum(units) as total_units_sold,
        sum(sales_amount) as total_sales_revenue,
        sum(cost_amount) as total_cost,
        (sum(sales_amount) - sum(cost_amount)) as profit,
        round((sum(sales_amount) - sum(cost_amount)) / nullif(sum(sales_amount), 0), 2) as profit_margin
    from sales_data
    group by product_id

),

product_info as (

    select
        product_id,
        product_name,
        category,
    from {{ ref('int_products_cleaned') }}
)

select
    a.product_id,
    pi.product_name,
    pi.category,
    a.total_units_sold,
    a.total_sales_revenue,
    a.total_cost,
    a.profit,
    a.profit_margin
from aggregated a
left join product_info pi
    on a.product_id = pi.product_id
order by a.profit_margin desc