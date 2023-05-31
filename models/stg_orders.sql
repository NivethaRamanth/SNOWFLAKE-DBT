{{
    config(
        materialized='incremental',
        unique_key='order_id',
        on_schema_change='append_new_columns',
    )
}}

select * from 
(select
    id as order_id,
    user_id as customer_id,
    order_date,
    status,
    _ETL_LOADED_AT as load_date,
    FLAG as flag,
    (ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY _ETL_LOADED_AT  DESC)) AS is_latest
    
from raw.jaffle_shop.orders
)
 
{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  where load_date > (CURRENT_DATE - 1)--(select max(load_date) from {{ this }})
    and is_latest = 1
{% endif %}