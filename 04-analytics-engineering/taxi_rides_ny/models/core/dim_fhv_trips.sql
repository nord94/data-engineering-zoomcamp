{{ config(materialized='table') }}

with dim_zones as (
    select 
        * 
    from {{ ref('dim_zones') }}
        where borough != 'Unknown'
), tmp as (
    select
        fhv.*
        , pu_zones.zone as pickup_zone
        , pu_zones.borough as pickup_borough
        , do_zones.zone as dropoff_zone
        , do_zones.borough as dropoff_borough
        , extract(year from (pickup_datetime)) as pu_year
        , extract(month from (pickup_datetime)) as pu_month
    from {{ ref('stg_fhv_tripdata')}} fhv
        join {{ ref('dim_zones')}} pu_zones on fhv.pulocationid = pu_zones.locationid
        join {{ ref('dim_zones')}} do_zones on fhv.dolocationid = do_zones.locationid
        where 1=1
            and extract(year from pickup_datetime) = 2019
)
select
    *
from tmp