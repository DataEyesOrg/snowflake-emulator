{{ config(materialized='incremental', unique_key='order_id') }}
select order_id, customer_id, status, amount_cents
from {{ ref('stg_orders') }}
