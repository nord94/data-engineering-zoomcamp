{{
    config(
        materialized='table'
    )
}}

with cleansed_data as (
	select
		fare_amount
		, extract (year from dropoff_datetime) as year
		, extract (month from dropoff_datetime) as month
		, 'green' service_type
	from {{ ref('stg_green_tripdata')}}
		where 1=1
			and fare_amount > 0 and trip_distance > 0 and payment_type_description in ('Cash', 'Credit Card') 
			and extract (year from dropoff_datetime) = 2020 and extract (month from dropoff_datetime) = 4
	 union all
	select
		fare_amount
		, extract (year from dropoff_datetime) as year
		, extract (month from dropoff_datetime) as month
		, 'yellow' service_type
	from {{ ref('stg_yellow_tripdata')}}
		where 1=1
			and fare_amount > 0 and trip_distance > 0 and payment_type_description in ('Cash', 'Credit Card')
			and extract (year from dropoff_datetime) = 2020 and extract (month from dropoff_datetime) = 4
)
SELECT
  service_type
 	, year
	, month
	, PERCENTILE_CONT(0.97) WITHIN GROUP (ORDER BY fare_amount) AS fare_97percentile
	, PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY fare_amount) AS fare_95percentile
	, PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY fare_amount) AS fare_90percentile
FROM cleansed_data
	group by service_type, year, month