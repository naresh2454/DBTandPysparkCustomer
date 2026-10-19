{{
    config(
        materialized='table',
        catalog='dbtandpyspark1',
        schema='gold'
    )
}}

with customers as (
    select * from {{ ref('silver_customers') }}
),

orders as (
    select * from {{ ref('silver_orders') }}
),

customer_stats as (
    select
        customer_id,
        count(order_id) as total_orders,
        sum(amount) as lifetime_value,
        avg(amount) as avg_order_value,
        max(order_order_date) as last_order_date -- adjust column name if needed based on your silver orders
    from orders
    group by customer_id
),

final as (
    select
        c.customer_id,
        c.customer_name,
        c.signup_timestamp,
        coalesce(s.total_orders, 0) as total_orders,
        coalesce(s.lifetime_value, 0.0) as lifetime_value,
        s.last_order_date,
        case when coalesce(s.lifetime_value, 0.0) > 500 then true else false end as is_high_value
    from customers c
    left join customer_stats s on c.customer_id = s.customer_id
)

select * from final