{% snapshot snapshot_stores %}
    {{
        config(
            target_schema='snapshots',
            unique_key='store_id',
            strategy='check',
            check_cols=['store_name','store_city','store_location','store_open_date']
        )
    }}
 
    select
        store_id, store_name, store_city,store_location, store_open_date
    from {{ ref('stg_stores') }}
{% endsnapshot %}