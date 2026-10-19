select
    order_id,
    customer_id,
    order_date,
    amount,
    _loaded_at
from {{ source('raw_system', 'raw_orders') }}