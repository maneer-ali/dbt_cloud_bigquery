WITH base_orders AS (
select
order_id, 
customer_id,
order_date,
order_status,
order_amount
from {{ source('bigquery_source', 'orders') }}
)

select
order_id, 
customer_id,
order_date,
order_status,
order_amount,
DATE_DIFF(current_date(), order_date, DAY) as day_since_order,
case
when order_status = 'Completed' then TRUE
else FALSE
END AS is_completed,
case
when order_amount > 300 then 'High'
when order_amount > 150 then 'Medium'
else 'Low'
end as order_value_tier
from base_orders