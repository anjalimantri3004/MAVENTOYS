{% snapshot snapshot_dictionary %}
    {{
        config(
            target_schema='snapshots',
            unique_key='table_name || \'-\' || field_name',
            strategy='check',
            check_cols=['field_description']
        )
    }}
 
    select
        table_name,
        field_name,
        field_description
    from {{ ref('stg_dictionary') }}
{% endsnapshot %}