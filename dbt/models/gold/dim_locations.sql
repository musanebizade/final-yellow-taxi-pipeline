{{
  config(
    materialized='table',
    schema='gold'
  )
}}

select * from {{ ref('taxi_zones') }}