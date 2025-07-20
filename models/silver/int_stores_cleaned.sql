{{ config(
    materialized = 'table',
    schema = 'public'
) }}

with source as (

    select * from {{ ref('stg_stores') }}

),

renamed as (

    select
        cast("Store_ID" as varchar) as store_id,
        initcap(trim("Store_Name")) as store_name,
        upper(trim("Location")) as location,
        cast(nullif("Capacity", '') as integer) as capacity,
        current_timestamp() as updated_at

    from source
    where "Store_ID" is not null and "Store_Name" is not null

)

select * from renamed