select
    order_id,
    customer_id,
    cast(order_date as date) as order_date,
    cast(amount as decimal(10, 2)) as amount
from {{ ref('bronze_orders') }}
qualify row_number() over (partition by order_id order by _loaded_at desc) = 1s