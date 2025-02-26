{{
    config(
        materialized='table'
    )
}}

with green_taxies as (
    select
        *
        , concat('Q',(extract (quarter from dropoff_datetime))::text, '/', (extract (year from dropoff_datetime))::text) as qr
    from {{ ref("stg_green_tripdata")}}
), yellow_taxies as (
    select
        *
        , concat('Q',(extract (quarter from dropoff_datetime))::text, '/', (extract (year from dropoff_datetime))::text) as qr
    from {{ ref("stg_yellow_tripdata")}}
), green_agg as (
    select
        qr
        , sum(total_amount) as qr_revenue
        , extract (year from dropoff_datetime) "year"
        , extract (quarter from dropoff_datetime) "quarter"
    from green_taxies
        group by qr, "year", "quarter"
), yellow_agg as (
    select
        qr
        , sum(total_amount) as qr_revenue
        , extract (year from dropoff_datetime) "year"
        , extract (quarter from dropoff_datetime) "quarter"
    from yellow_taxies
        group by qr, "year", "quarter"
), fct_taxi_trips_quarterly_revenue as (
    select
        qr
        , qr_revenue
        , "quarter"
        , "year"
        , 'green' taxi_color
    from green_agg
    union all
    select
        qr
        , qr_revenue
        , "quarter"
        , "year"
        , 'yellow' taxi_color
    from yellow_agg
),  cte as (
	select
		*
	from fct_taxi_trips_quarterly_revenue fttqr
		where 1=1
			and year between 2019 and 2020
), cte_preagg as (
	select
		cte_next.*
		, cte_prev.qr_revenue prev_qr_revenue
		, case
			when cte_prev.qr_revenue is null then null
			else 100*round((cte_next.qr_revenue - cte_prev.qr_revenue)/cte_prev.qr_revenue, 4)
		end as quarterly_yoy
	from cte cte_next
		left join cte cte_prev on cte_next.year = cte_prev."year" + 1 and cte_prev.quarter = cte_next.quarter 
			and cte_next.taxi_color = cte_prev.taxi_color
		where cte_prev.qr_revenue is not null
		order by cte_next.taxi_color, cte_next.year, cte_next.quarter
), final_cte as (
	select
		*
		, row_number() over(partition by taxi_color order by quarterly_yoy desc) rn
	from cte_preagg
)
select
	qr
	, taxi_color
	, qr_revenue 
	, prev_qr_revenue
	, quarterly_yoy
	, concat('best quarter for ', taxi_color, 'is ', qr)
from final_cte
	where rn = 1
 union all
select
	qr
	, taxi_color
	, qr_revenue 
	, prev_qr_revenue
	, quarterly_yoy
	, concat('worst quarter for ', taxi_color, 'is ', qr)
from final_cte
	where rn = 4;
