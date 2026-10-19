select
    id as customer_id,
    name,
    signup_date,
    _loaded_at
from {{ source('raw_system', 'raw_customers') }}
