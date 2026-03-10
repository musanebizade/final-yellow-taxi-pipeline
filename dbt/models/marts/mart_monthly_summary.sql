{{
  config(
    materialized='table',
    schema='mart'
  )
}}

select
    date_trunc('month', t.tpep_pickup_datetime) as ride_month,
    v.vendor_name,
    r.rate_description,
    count(*) as ride_count,
    round(avg(t.trip_duration_minutes), 2) as avg_trip_duration_minutes,
    round(avg(t.total_amount), 2) as avg_total_amount,
    round(avg(t.trip_distance), 2) as avg_trip_distance
from {{ ref('fct_trips') }} t
left join {{ ref('dim_vendor') }} v on t.vendorid = v.vendor_id
left join {{ ref('dim_rate') }} r on t.ratecodeid = r.rate_id
group by 1, 2, 3
order by 1

-- group by 1, 2, 3 means group by the 1st, 2nd, and 3rd columns in your select statement, which are:

-- 1 = ride_month
-- 2 = vendor_name
-- 3 = rate_description

-- order by 1`** means sort by the 1st column which is `ride_month`. 
-- So results are sorted chronologically from oldest to newest month.
