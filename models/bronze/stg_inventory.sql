{{ config(
    materialized='table',
    schema='public'
) }}

with source_data as (

    select
        cast('Store_ID' as varchar) as store_id,
        cast('Product_ID' as varchar) as product_id,
        try_cast('Stock_On_Hand' as integer) as stock_on_hand,
        
    from {{ source('public', 'inventory') }}

)

select * from source_data