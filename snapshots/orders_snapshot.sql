{% snapshot orders_snapshot %}

    {{
        config(
          target_database='snowflake_training',
          target_schema='snapshots',
          strategy='timestamp',
          unique_key='id',
          updated_at='_ETL_LOADED_AT',
          invalidate_hard_deletes=True,
        )
    }}

    select * from {{ source('jaffle_shop', 'orders') }}

{% endsnapshot %}