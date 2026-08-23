-- parse the messy dimension (-- →null, 50+ ratings→50, ₹ 200→200, city after last comma):

select
    id::bigint as restaurant_id,
    name as restaurant_name,
    trim(coalesce(regexp_substr(city, '[^,]+$'), city)) as city,
    try_cast(nullif(rating, '--') as decimal(3,1)) as rating,
    try_cast(regexp_substr(rating_count, '[0-9]+') as bigint) as rating_count,
    try_cast(regexp_substr(cost, '[0-9]+') as bigint) as cost_for_two,
    cuisine,
    lic_no as license_no
from {{ source('bronze', 'restaurants') }} where try_cast(id as bigint) is not null