{% snapshot snapshot_dictionary %}
    {{
        config(
            target_schema='snapshots',
            unique_key='table_name || \'-\' || field',
            strategy='check',
            check_cols=['description']
        )
    }}
 
    select
        table_name,
        field,
        description
    from {{ ref('stg_dictionary') }}
{% endsnapshot %}