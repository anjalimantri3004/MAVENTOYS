{{ config(
    materialized = 'table',
    schema = 'public'
) }}

with source as (

    select * from {{ ref('stg_stores') }}

),

renamed as (

    select
        cast('STORE_ID' as varchar) as store_id,
        initcap(trim('STORE_NAME')) as store_name,
        upper(trim('LOCATION')) as location,
        cast(nullif('CAPACITY', '') as integer) as capacity,
        current_timestamp() as updated_at

    from source
    where "Store_ID" is not null and "Store_Name" is not null

)

select * from renamed