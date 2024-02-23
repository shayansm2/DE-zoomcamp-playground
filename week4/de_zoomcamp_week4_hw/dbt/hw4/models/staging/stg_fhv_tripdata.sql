{{
    config(
        materialized='view'
    )
}}

with tripdata as 
(
    select *,
    row_number() over(partition by dispatching_base_num, pickup_datetime) as rn
    from {{ source('staging','fhv_tripdata') }}
    where extract(year from cast(pickup_datetime as date)) = 2019
)

select
    {{ dbt_utils.generate_surrogate_key(['dispatching_base_num', 'pickup_datetime']) }} as tripid,
    dispatching_base_num as dispatching_base_number,
    {{ dbt.safe_cast("pulocationid", api.Column.translate_type("integer")) }} as pickup_locationid,
    {{ dbt.safe_cast("dolocationid", api.Column.translate_type("integer")) }} as dropoff_locationid,
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropoff_datetime as timestamp) as dropoff_datetime,
    sr_flag as flag,
    affiliated_base_number
from tripdata
where rn = 1