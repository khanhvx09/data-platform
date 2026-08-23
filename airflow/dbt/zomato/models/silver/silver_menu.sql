select
menu_id,
try_cast(r_id as bigint) as restaurant_id,
f_id,
cuisine,
try_cast(price as decimal(10,2)) as price
from {{ source('bronze', 'menu') }} where try_cast(r_id as bigint) is not null and try_cast(price as decimal(10,2)) > 0