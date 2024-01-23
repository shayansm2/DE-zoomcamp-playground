## Module 1 Homework

## Question 1. Knowing docker tags

Which tag has the following text? - *Automatically remove the container when it exits* 

*answer:* 
```bash
docker run --help | grep "Automatically remove the container when it exits"
```

- [ ] `--delete`
- [ ] `--rc`
- [ ] `--rmc`
- [x] `--rm`


## Question 2. Understanding docker first run 

Run docker with the python:3.9 image in an interactive mode and the entrypoint of bash.
Now check the python modules that are installed ( use ```pip list``` ). 
What is version of the package *wheel* ?

*answer:*
```bash
docker run -it --rm --entrypoint=bash python:3.9
pip list | grep wheel
```

- [x] 0.42.0
- [ ] 1.0.0
- [ ] 23.0.1
- [ ] 58.1.0


# Prepare Postgres

[loading data from the links to postgress db](./pipeline.ipynb)

## Question 3. Count records 

How many taxi trips were totally made on September 18th 2019?

*answer:*
```sql
select count(1)
from ny_taxi.public.green_taxi_data
where lpep_pickup_datetime >= '2019-09-18'
  and lpep_pickup_datetime < '2019-09-19';
```

- [x] 15767
- [ ] 15612
- [ ] 15859
- [ ] 89009

## Question 4. Largest trip for each day

Which was the pick up day with the largest trip distance
Use the pick up time for your calculations.

*answer:*
```sql
select date(lpep_pickup_datetime)
from ny_taxi.public.green_taxi_data
where EXTRACT(EPOCH FROM TO_TIMESTAMP(lpep_dropoff_datetime, 'YYYY-MM-DD HH24:MI:SS') -
                         TO_TIMESTAMP(lpep_pickup_datetime, 'YYYY-MM-DD HH24:MI:SS'))::INTEGER =
      (select max(EXTRACT(EPOCH FROM TO_TIMESTAMP(lpep_dropoff_datetime, 'YYYY-MM-DD HH24:MI:SS') -
                                     TO_TIMESTAMP(lpep_pickup_datetime, 'YYYY-MM-DD HH24:MI:SS'))::INTEGER)
       from ny_taxi.public.green_taxi_data);
```

- [ ] 2019-09-18
- [ ] 2019-09-16
- [x] 2019-09-26
- [ ] 2019-09-21


## Question 5. Three biggest pick up Boroughs

Consider lpep_pickup_datetime in '2019-09-18' and ignoring Borough has Unknown

Which were the 3 pick up Boroughs that had a sum of total_amount superior to 50000?

*answer:*
```sql
select tz."Borough", count(1), sum(gtd.total_amount)
from ny_taxi.public.green_taxi_data gtd
         join ny_taxi.public.taxi_zones tz on tz."LocationID" = gtd."PULocationID"
where DATE(lpep_pickup_datetime) = '2019-09-18'
group by tz."Borough"
having sum(gtd.total_amount) > 50000
order by count(1) desc;
```
 
- [x] "Brooklyn" "Manhattan" "Queens"
- [ ] "Bronx" "Brooklyn" "Manhattan"
- [ ] "Bronx" "Manhattan" "Queens" 
- [ ] "Brooklyn" "Queens" "Staten Island"


## Question 6. Largest tip

For the passengers picked up in September 2019 in the zone name Astoria which was the drop off zone that had the largest tip?
We want the name of the zone, not the id.

*answer:*
```sql
select tip_amount, dotz."Zone"
from ny_taxi.public.green_taxi_data gtd
         join ny_taxi.public.taxi_zones putz on gtd."PULocationID" = putz."LocationID"
         join ny_taxi.public.taxi_zones dotz on gtd."DOLocationID" = dotz."LocationID"
where to_date(lpep_pickup_datetime, 'YYYY-MM') = '2019-09-01'
  and putz."Zone" = 'Astoria'
order by tip_amount desc
limit 1;
```

- [ ] Central Park
- [ ] Jamaica
- [x] JFK Airport
- [ ] Long Island City/Queens Plaza



## Terraform

In this section homework we'll prepare the environment by creating resources in GCP with Terraform.

In your VM on GCP/Laptop/GitHub Codespace install Terraform. 
Copy the files from the course repo
[here](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/01-docker-terraform/1_terraform_gcp/terraform) to your VM/Laptop/GitHub Codespace.

Modify the files as necessary to create a GCP Bucket and Big Query Dataset.


## Question 7. Creating Resources

After updating the main.tf and variable.tf files run:

```
terraform apply
```

Paste the output of this command into the homework submission form.


## Submitting the solutions

* Form for submitting: https://courses.datatalks.club/de-zoomcamp-2024/homework/hw01
* You can submit your homework multiple times. In this case, only the last submission will be used. 

Deadline: 29 January, 23:00 CET