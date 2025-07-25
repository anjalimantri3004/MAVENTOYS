{{ config(
    materialized='table',
    schema='gold'
) }}

with latest_inventory as (

    select
        product_id,
        store_id,
        stock_on_hand,
        last_updated,
        row_number() over (
            partition by product_id, store_id
            order by last_updated desc
        ) as row_num

    from {{ ref('int_inventory_status') }}

),

cleaned_inventory as (

    select
        product_id,
        store_id,
        stock_on_hand,
        last_updated,
        case
            when stock_on_hand = 0 then 'Out of Stock'
            else 'In Stock'
        end as stock_status
    from latest_inventory
    where row_num = 1
)

select * from cleaned_inventory