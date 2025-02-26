{{ config(materialized='table') }}

with tmp as (
    select
        *
        , EXTRACT(EPOCH FROM (dropoff_datetime - pickup_datetime)) AS trip_duration
    from {{ ref('dim_fhv_trips')}}
), agg_cte as (
    select
        pu_year
        , pu_month
        , pickup_zone
        , dropoff_zone
        , PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY trip_duration) AS p90_trip_duration
    from tmp
        group by pu_year, pu_month, pickup_zone, dropoff_zone
)
select * from agg_cte