## Week 3 Homework
## Question 1:
Question 1: What is count of records for the 2022 Green Taxi Data??
- [ ] 65,623,481
- [x] 840,402
- [ ] 1,936,423
- [ ] 253,647

```SQL
select count(1)
from green_taxi_data;
```

## Question 2:
Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.</br> 
What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?

- 0 MB for the External Table and 6.41MB for the Materialized Table
- 18.82 MB for the External Table and 47.60 MB for the Materialized Table
- 0 MB for the External Table and 0MB for the Materialized Table
- 2.14 MB for the External Table and 0MB for the Materialized Table

```sql
select count(distinct pulocationid)
from green_taxi_data;
```

## Question 3:
How many records have a fare_amount of 0?
- [ ] 12,488
- [ ] 128,219
- [ ] 112
- [x] 1,622

```sql
select count(1)
from green_taxi_data
where fare_amount = 0;
```

## Question 4:
What is the best strategy to make an optimized table in Big Query if your query will always order the results by PUlocationID and filter based on lpep_pickup_datetime? (Create a new table with this strategy)
- [ ] Cluster on lpep_pickup_datetime Partition by PUlocationID
- [x] Partition by lpep_pickup_datetime  Cluster on PUlocationID
- [ ] Partition by lpep_pickup_datetime and Partition by PUlocationID
- [ ] Cluster on by lpep_pickup_datetime and Cluster on PUlocationID

```SQL
-- create partioned table
CREATE TABLE green_taxi_monthly_clustered (LIKE green_taxi_data INCLUDING ALL)
        PARTITION BY RANGE (date_trunc('day', lpep_pickup_datetime));

-- create monthly partitions
CREATE TABLE green_taxi_monthly_clustered_{month} PARTITION OF postgres.public.green_taxi_monthly_clustered
        FOR VALUES FROM ('{first_day}') TO ('{last_day}');

-- add constrainst for partitions
ALTER TABLE green_taxi_monthly_clustered_{month} ADD CONSTRAINT green_taxi_monthly_clustered_{month}_check
        CHECK (lpep_pickup_datetime >= '{first_day}' AND lpep_pickup_datetime < '{last_day}');

-- move data from original table to partitioned table
INSERT INTO postgres.public.green_taxi_monthly_clustered
SELECT *
FROM (select *
      from postgres.public.green_taxi_data
      where lpep_pickup_datetime >= '2022-01-01'
        and lpep_pickup_datetime < '2023-01-01') as "cleaned_data";

-- clustering
CREATE INDEX pulocationid_index on postgres.public.green_taxi_data (pulocationid);
CLUSTER postgres.public.green_taxi_data USING pulocationid_index;
```

## Question 5:
Write a query to retrieve the distinct PULocationID between lpep_pickup_datetime
06/01/2022 and 06/30/2022 (inclusive)</br>

Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 4 and note the estimated bytes processed. What are these values? </br>

Choose the answer which most closely matches.</br> 

- 22.82 MB for non-partitioned table and 647.87 MB for the partitioned table
- 12.82 MB for non-partitioned table and 1.12 MB for the partitioned table
- 5.63 MB for non-partitioned table and 0 MB for the partitioned table
- 10.31 MB for non-partitioned table and 10.31 MB for the partitioned table

```SQL
explain
select distinct pulocationid
from green_taxi_data
where lpep_pickup_datetime between '2022-06-01' and '2022-06-30';

explain
select distinct pulocationid
from green_taxi_monthly_clustered
where lpep_pickup_datetime between '2022-06-01' and '2022-06-30';
```

## Question 6: 
Where is the data stored in the External Table you created?

- [ ] Big Query
- [x] GCP Bucket
- [ ] Big Table
- [ ] Container Registry


## Question 7:
It is best practice in Big Query to always cluster your data:
- [ ] True
- [x] False
