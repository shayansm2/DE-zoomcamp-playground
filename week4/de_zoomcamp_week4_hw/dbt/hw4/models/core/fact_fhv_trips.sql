{{
    config(
        materialized='table'
    )
}}

with dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
),
 
fhv_tripdata as (
    select *
    from {{ ref('stg_fhv_tripdata') }}
)

select fhv_tripdata.tripid, 
    fhv_tripdata.dispatching_base_number,
    fhv_tripdata.pickup_locationid,
    fhv_tripdata.dropoff_locationid,
    fhv_tripdata.pickup_datetime,
    fhv_tripdata.dropoff_datetime,
    fhv_tripdata.flag,
    fhv_tripdata.affiliated_base_number,
    pickup_zone.borough as pickup_borough, 
    pickup_zone.zone as pickup_zone, 
    dropoff_zone.borough as dropoff_borough, 
    dropoff_zone.zone as dropoff_zone  
from fhv_tripdata
inner join dim_zones as pickup_zone
on fhv_tripdata.pickup_locationid = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on fhv_tripdata.dropoff_locationid = dropoff_zone.locationid