select
    r.city,
    hour(f.order_timestamp) as order_hour,
    count_if(f.is_delivered) as delivered_orders,
    round(median(f.delivery_time_min), 1) as p50,
    round(percentile_cont(0.9) within group (order by f.delivery_time_min), 1) as p90
from {{ ref('fct_orders') }} f
left join {{ ref('dim_restaurant') }} r using (restaurant_id)
where f.is_delivered
group by 1, 2
