{{
  config(
    materialized='table',
    schema='gold'
  )
}}

select
    t.vendorid,
    t.ratecodeid,
    t.tpep_pickup_datetime,
    t.tpep_dropoff_datetime,
    t.passenger_count,
    t.trip_distance,
    t.total_amount,
    t.tip_amount,
    t.payment_type,
    t.pickup_longitude,
    t.pickup_latitude,
    t.trip_duration_minutes,
    t.time_of_day,
    t.pickup_location_id
from {{ ref('stg_trips') }} t

--t is a table alias. It's a shorthand name you give to a table so you don't have to write the full name every time.