with orders as (
    select customer_id, count(*) as order_count, sum(amount_cents) as total_cents
    from {{ ref('stg_orders') }}
    group by customer_id
)

select
    c.id as customer_id,
    c.name,
    c.joined_on,
    coalesce(o.order_count, 0) as order_count,
    coalesce(o.total_cents, 0) as total_cents
from {{ ref('raw_customers') }} as c
left join orders as o on o.customer_id = c.id
