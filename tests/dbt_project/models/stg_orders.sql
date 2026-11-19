{{ config(materialized='view') }}
select
    id as order_id,
    customer_id,
    ordered_on,
    status,
    amount_cents
from {{ ref('raw_orders') }}
