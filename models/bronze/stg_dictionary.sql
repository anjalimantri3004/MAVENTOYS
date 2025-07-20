{{ config(
    materialized='table',
    schema='public'
) }}

with source_data as (

    select
        trim("C1") as table_name,
        trim("C2") as field_name,
        trim("C3") as field_description,
        

    from {{ source('public', 'dictionary') }}

)

select * from source_data