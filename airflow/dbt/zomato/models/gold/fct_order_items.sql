{{ config(
    materialized='incremental',
    unique_key='order_item_id',
    incremental_strategy='merge',
    on_schema_change='append_new_columns'
) }}

select
    oi.order_item_id,
    oi.order_id,
    oi.restaurant_id,
    oi.f_id,
    o.customer_id,
    o.order_timestamp as order_ts,
    o.order_date,
    oi.price,
    oi.quantity,
    oi.line_amount
from {{ ref('silver_order_items') }} oi
inner join {{ ref('silver_orders') }} o using (order_id)

{% if is_incremental() %}
where o.order_timestamp > (select coalesce(max(order_ts), '1900-01-01'::timestamp) from {{ this }})
{% endif %}
