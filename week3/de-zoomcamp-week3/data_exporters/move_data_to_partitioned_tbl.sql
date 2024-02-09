INSERT INTO postgres.public.green_taxi_monthly_clustered
SELECT *
FROM (select *
      from postgres.public.green_taxi_data
      where lpep_pickup_datetime >= '2022-01-01'
        and lpep_pickup_datetime < '2023-01-01') as "cleaned_data";