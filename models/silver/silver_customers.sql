select
    customer_id,
    initcap(trim(name)) as customer_name,
    cast(signup_date as timestamp) as signup_timestamp
from {{ ref('bronze_customers') }}
qualify row_number() over (partition by customer_id order by _loaded_at desc) = 1