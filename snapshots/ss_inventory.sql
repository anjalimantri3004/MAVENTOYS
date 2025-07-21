{% snapshot snapshot_inventory %}
    {{
        config(
            target_schema='snapshots',
            unique_key='store_id || \'-\' || product_id',
            strategy='check',
            check_cols=['stock_on_hand']
        )
    }}
 
    select  
        store_id,
        product_id,
        stock_in_hand
    from {{ ref('stg_inventory') }}
{% endsnapshot %}
 