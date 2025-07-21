% snapshot snapshot_products %}
    {{
        config(
            target_schema='snapshots',
            unique_key='product_id',
            strategy='check',
            check_cols=['product_name', 'product_category', 'product_cost', 'product_price']
        )
    }}
 
    select
        product_id,
        product_name,
        product_category,
        product_cost,
        product_price
    from {{ ref('stg_products') }}
{% endsnapshot %}