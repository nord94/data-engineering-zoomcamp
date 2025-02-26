{{
    config(
        materialized='view'
    )
}}



select
    dispatching_base_num
    , pickup_datetime
    , "dropOff_datetime" as dropoff_datetime
    , "PUlocationID" as pulocationid
    , "DOlocationID" as dolocationid
    , "SR_Flag" as sr_flag
    , "Affiliated_base_number" as affiliated_base_number
from {{ source('staging','fhv_taxies') }}
    where 1=1
        and dispatching_base_num is not null
