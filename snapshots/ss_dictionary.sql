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
        field_name,
        field_descriptiondescription
    from {{ ref('stg_dictionary') }}
{% endsnapshot %}