select
    f.order_date,
    r.city,
    count(*) as orders,
    count_if(f.is_delivered) as delivered_orders,
    round(coalesce(try_divide(count_if(f.order_status = 'Cancelled'), count(*)), 0), 4) as cancel_rate,
    sum(if(f.is_delivered, f.sales_amount, 0)) as gmv,
    round(coalesce(try_divide(sum(if(f.is_delivered, f.sales_amount, 0)), count_if(f.is_delivered)), 0), 2) as aov
from {{ ref('fct_orders') }} f
left join {{ ref('dim_restaurant') }} r using (restaurant_id)
group by 1, 2
