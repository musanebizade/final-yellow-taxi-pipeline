{{
  config(
    materialized='table',
    schema='mart'
  )
}}

select
    p.payment_method,
    count(*) as trip_count,
    round(avg(t.total_amount), 2) as avg_total_amount,
    round(avg(t.tip_amount), 2) as avg_tip_amount,
    round(sum(t.total_amount), 2) as total_revenue
from {{ ref('fct_trips') }} t
left join {{ ref('payment_types') }} p
    on t.payment_type = p.payment_type_id
group by p.payment_method
order by trip_count desc