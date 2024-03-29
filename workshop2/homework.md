# Homework

## Setting up

In order to get a static set of results, we will use historical data from the dataset.

Run the following commands:
```bash
# Load the cluster op commands.
source commands.sh
# First, reset the cluster:
clean-cluster
# Start a new cluster
start-cluster
# wait for cluster to start
sleep 5
# Seed historical data instead of real-time data
seed-kafka
# Recreate trip data table
psql -f risingwave-sql/table/trip_data.sql
# Wait for a while for the trip_data table to be populated.
sleep 5
# Check that you have 100K records in the trip_data table
# You may rerun it if the count is not 100K
psql -c "SELECT COUNT(*) FROM trip_data"
```

## Question 0

_This question is just a warm-up to introduce dynamic filter, please attempt it before viewing its solution._

What are the dropoff taxi zones at the latest dropoff times?

For this part, we will use the [dynamic filter pattern](https://docs.risingwave.com/docs/current/sql-pattern-dynamic-filters/).

first answer:
```sql
select taxi_zone.zone, tpep_dropoff_datetime
from trip_data
         join taxi_zone on taxi_zone.location_id = trip_data.dolocationid
where tpep_dropoff_datetime = (select max(tpep_dropoff_datetime)
                               from trip_data);
```

using **dynamic filter pattern** (`WITH`) and **materialized view**:

```sql
CREATE MATERIALIZED VIEW latest_dropoff_time AS
WITH t AS (SELECT MAX(tpep_dropoff_datetime) AS latest_dropoff_time
           FROM trip_data)
SELECT taxi_zone.Zone as taxi_zone, latest_dropoff_time
FROM t,
     trip_data
         JOIN taxi_zone
              ON trip_data.DOLocationID = taxi_zone.location_id
WHERE trip_data.tpep_dropoff_datetime = t.latest_dropoff_time;
```

## Question 1

Create a materialized view to compute the average, min and max trip time **between each taxi zone**.

Note that we consider the do not consider `a->b` and `b->a` as the same trip pair.
So as an example, you would consider the following trip pairs as different pairs:
```plaintext
Yorkville East -> Steinway
Steinway -> Yorkville East
```

From this MV, find the pair of taxi zones with the highest average trip time.
You may need to use the [dynamic filter pattern](https://docs.risingwave.com/docs/current/sql-pattern-dynamic-filters/) for this.

```sql
create materialized view taxi_zones_statistics as
with data as (select zdo.zone                                     as drop_off_zone,
                     zpu.zone                                     as pick_up_zone,
                     tpep_dropoff_datetime - tpep_pickup_datetime as trip_time
              from trip_data as t
                       join taxi_zone as zdo on zdo.location_id = t.DOLocationID
                       join taxi_zone as zpu on zpu.location_id = t.pulocationid)
select pick_up_zone,
       drop_off_zone,
       TO_CHAR(min(trip_time), 'HH24:MI:SS') as min_trip_time,
       TO_CHAR(max(trip_time), 'HH24:MI:SS') as max_trip_time,
       TO_CHAR(avg(trip_time), 'HH24:MI:SS') as avg_trip_time
from data
group by drop_off_zone, pick_up_zone;



with highest_avg_trip_time as (select max(avg_trip_time) as max_avg_time
                               from taxi_zones_statistics)
select pick_up_zone, drop_off_zone
from taxi_zones_statistics
         join highest_avg_trip_time
              on avg_trip_time = highest_avg_trip_time.max_avg_time;


-- Yorkville East -> Steinway
```
Options:
- [x] Yorkville East, Steinway
- [ ] Murray Hill, Midwood
- [ ] East Flatbush/Farragut, East Harlem North
- [ ] Midtown Center, University Heights/Morris Heights

Bonus (no marks): Create an MV which can identify anomalies in the data. For example, if the average trip time between two zones is 1 minute,
but the max trip time is 10 minutes and 20 minutes respectively.

```sql
create materialized view trip_anomalies as
select *
from taxi_zones_statistics
where avg_trip_time <= '00:01:00'
  and max_trip_time >= '00:10:00';
```

## Question 2

Recreate the MV(s) in question 1, to also find the **number of trips** for the pair of taxi zones with the highest average trip time.

```sql
drop materialized view taxi_zones_statistics;

create materialized view taxi_zones_statistics as
with data as (select zdo.zone                                     as drop_off_zone,
                     zpu.zone                                     as pick_up_zone,
                     tpep_dropoff_datetime - tpep_pickup_datetime as trip_time
              from trip_data as t
                       join taxi_zone as zdo on zdo.location_id = t.DOLocationID
                       join taxi_zone as zpu on zpu.location_id = t.pulocationid)
select pick_up_zone,
       drop_off_zone,
       TO_CHAR(min(trip_time), 'HH24:MI:SS') as min_trip_time,
       TO_CHAR(max(trip_time), 'HH24:MI:SS') as max_trip_time,
       TO_CHAR(avg(trip_time), 'HH24:MI:SS') as avg_trip_time,
       count(1)                              as count
from data
group by drop_off_zone, pick_up_zone;



with highest_avg_trip_time as (select max(avg_trip_time) as max_avg_time
                               from taxi_zones_statistics)
select pick_up_zone, drop_off_zone, count
from taxi_zones_statistics
         join highest_avg_trip_time
              on avg_trip_time = highest_avg_trip_time.max_avg_time;
```

Options:
- [ ] 5
- [ ] 3
- [ ] 10
- [x] 1

## Question 3

From the latest pickup time to 17 hours before, what are the top 3 busiest zones in terms of number of pickups?
For example if the latest pickup time is 2020-01-01 17:00:00,
then the query should return the top 3 busiest zones from 2020-01-01 00:00:00 to 2020-01-01 17:00:00.

HINT: You can use [dynamic filter pattern](https://docs.risingwave.com/docs/current/sql-pattern-dynamic-filters/)
to create a filter condition based on the latest pickup time.

NOTE: For this question `17 hours` was picked to ensure we have enough data to work with.

```sql
with data as (select zpu.zone as pick_up_zone,
                     tpep_pickup_datetime
              from trip_data as t
                       join taxi_zone as zpu on zpu.location_id = t.pulocationid),
     max_pickup_time as (select max(tpep_pickup_datetime) as time
                         from trip_data)
select pick_up_zone
from data
         join max_pickup_time on data.tpep_pickup_datetime <= max_pickup_time.time
    and data.tpep_pickup_datetime >= max_pickup_time.time - INTERVAL '17 hours'
group by pick_up_zone
order by count(1) desc limit 3;
```

Options:
- [ ] Clinton East, Upper East Side North, Penn Station
- [x] LaGuardia Airport, Lincoln Square East, JFK Airport
- [ ] Midtown Center, Upper East Side South, Upper East Side North
- [ ] LaGuardia Airport, Midtown Center, Upper East Side North