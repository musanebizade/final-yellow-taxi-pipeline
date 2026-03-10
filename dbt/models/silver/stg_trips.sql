{{
  config(
    materialized='table',
    schema='silver'
  )
}}

select
    "VendorID"::integer as vendorid,
    "RateCodeID"::integer as ratecodeid,
    tpep_pickup_datetime::timestamp,
    tpep_dropoff_datetime::timestamp,
    passenger_count::integer,
    trip_distance::numeric,
    total_amount::numeric,
    tip_amount::numeric,
    payment_type::integer,
    pickup_longitude::numeric,
    pickup_latitude::numeric,

    -- Calculate trip duration in minutes
    round(
        extract(epoch from (tpep_dropoff_datetime - tpep_pickup_datetime)) / 60
    , 2) as trip_duration_minutes, 

    -- Categorize pickup time of day
    case
        when extract(hour from tpep_pickup_datetime) between 6 and 11 then 'morning'
        when extract(hour from tpep_pickup_datetime) between 12 and 17 then 'afternoon'
        when extract(hour from tpep_pickup_datetime) between 18 and 21 then 'evening'
        else 'night'
    end as time_of_day,

    -- Map coordinates to a pickup location ID based on defined zones
    case
        when pickup_latitude between 40.50 and 40.65 and pickup_longitude between -74.30 and -74.00 then 1
        when pickup_latitude between 40.50 and 40.65 and pickup_longitude between -74.00 and -73.85 then 2
        when pickup_latitude between 40.50 and 40.65 and pickup_longitude between -73.85 and -73.70 then 3
        when pickup_latitude between 40.65 and 40.75 and pickup_longitude between -74.30 and -74.00 then 4
        when pickup_latitude between 40.65 and 40.75 and pickup_longitude between -74.00 and -73.85 then 5
        when pickup_latitude between 40.65 and 40.75 and pickup_longitude between -73.85 and -73.70 then 6
        when pickup_latitude between 40.75 and 40.90 and pickup_longitude between -74.30 and -74.00 then 7
        when pickup_latitude between 40.75 and 40.90 and pickup_longitude between -74.00 and -73.85 then 8
        when pickup_latitude between 40.75 and 40.90 and pickup_longitude between -73.85 and -73.70 then 9
        else 0
    end as pickup_location_id
from {{ source('raw', 'taxi_trips') }}

-- Data validation and outlier removal
where passenger_count > 0
  and trip_distance > 0
  and total_amount > 0
  and "RateCodeID" != 99
  and payment_type in (1, 2, 3, 4)  -- Valid payment types
  and tpep_dropoff_datetime > tpep_pickup_datetime  -- Ensure chronological logic
  and pickup_latitude between 40.5 and 40.9         -- Filter valid NYC latitudes
  and pickup_longitude between -74.3 and -73.7      -- Filter valid NYC longitudes