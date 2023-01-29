-- q3
select count(1)
from green_taxi_trips
where lpep_pickup_datetime >= '2019-01-15 00:00:00'
  and lpep_pickup_datetime < '2019-01-16 00:00:00';


-- q4
select lpep_pickup_datetime
from green_taxi_trips
where trip_distance = (select max(trip_distance) from green_taxi_trips);


-- q5
select passenger_count, count(1)
from green_taxi_trips
where lpep_pickup_datetime >= '2019-01-01 00:00:00'
  and lpep_pickup_datetime < '2019-01-02 00:00:00' group by passenger_count


-- q6
