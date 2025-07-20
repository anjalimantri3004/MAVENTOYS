{{ config(
    materialized = 'table',
    schema = 'public'
) }}

with source as (

    select * from {{ ref('stg_stores') }}

),

renamed as (

    select
        cast(store_id as varchar) as store_id,
        initcap(trim(store_name)) as store_name,
        upper(trim(store_location)) as location,
        try_cast(capacity as integer) as capacity,
        current_timestamp() as updated_at

    from source
    where store_id is not null and store_name is not null

)

select * from renamed