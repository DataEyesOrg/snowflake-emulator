{{ config(materialized='incremental', unique_key='order_id', incremental_strategy='delete+insert') }}
select order_id, status
from {{ ref('stg_orders') }}
