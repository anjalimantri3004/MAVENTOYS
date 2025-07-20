{{ config(
    materialized='table',
    schema='public'
) }}

with source_data as (

    select
        cast("Product_ID" as varchar) as product_id,
        cast("Product_Name" as varchar) as product_name,
        cast("Category" as varchar) as category,
        try_cast("Price" as numeric(10,2)) as price,
        cast("Brand" as varchar) as brand

    from {{ ref('stg_products') }}

)

select * from source_data