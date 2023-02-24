{{ config(materialized='table') }}


with fhv_data as (
    select *, 
        'Fhv' as service_type 
    from {{ ref('stg_fhv_tripdata') }}
), 

dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)

select 
fhv_data.tripid as tripid, 
fhv_data.pickup_locationid,
fhv_data.pickup_datetime,
pickup_zone.borough as pickup_borough, 
pickup_zone.zone as pickup_zone,
fhv_data.dropoff_locationid,
fhv_data.dropoff_datetime,
dropoff_zone.borough as dropoff_borough,
dropoff_zone.zone as dropoff_zone, 
fhv_data.dispatching_base_num,
fhv_data.affiliated_base_numberl,
fhv_data.sr_flag,
from fhv_data 
inner join dim_zones as pickup_zone
on fhv_data.pickup_locationid = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on fhv_data.dropoff_locationid = dropoff_zone.locationid
where extract(YEAR FROM pickup_datetime)=2019

