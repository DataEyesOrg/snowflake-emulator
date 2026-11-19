{% snapshot orders_history %}
{{ config(target_schema='history', unique_key='order_id', strategy='check', check_cols=['status']) }}
select order_id, status from {{ ref('stg_orders') }}
{% endsnapshot %}
