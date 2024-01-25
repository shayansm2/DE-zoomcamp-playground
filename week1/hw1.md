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

[terraform file](./main.tf)


## Question 7. Creating Resources

After updating the main.tf and variable.tf files run:

```
terraform apply
```

Paste the output of this command into the homework submission form.

```
kubernetes_namespace.example: Refreshing state... [id=de-zoomcamp-namespace-week1]
kubernetes_deployment.example: Refreshing state... [id=de-zoomcamp-namespace-week1/bigquery-emulator]

Terraform used the selected providers to generate the following execution plan. Resource actions are indicated with the following symbols:
-/+ destroy and then create replacement

Terraform will perform the following actions:

  # kubernetes_deployment.example is tainted, so must be replaced
-/+ resource "kubernetes_deployment" "example" {
      ~ id               = "de-zoomcamp-namespace-week1/bigquery-emulator" -> (known after apply)
        # (1 unchanged attribute hidden)

      ~ metadata {
          - annotations      = {} -> null
          ~ generation       = 1 -> (known after apply)
          - labels           = {} -> null
            name             = "bigquery-emulator"
          ~ resource_version = "19192" -> (known after apply)
          ~ uid              = "73a1c837-e1dd-4332-94dc-117e4545f72d" -> (known after apply)
            # (1 unchanged attribute hidden)
        }

      ~ spec {
            # (5 unchanged attributes hidden)

          - strategy {
              - type = "RollingUpdate" -> null

              - rolling_update {
                  - max_surge       = "25%" -> null
                  - max_unavailable = "25%" -> null
                }
            }

          ~ template {
              ~ metadata {
                  - annotations      = {} -> null
                  ~ generation       = 0 -> (known after apply)
                  + name             = (known after apply)
                  + resource_version = (known after apply)
                  + uid              = (known after apply)
                    # (1 unchanged attribute hidden)
                }
              ~ spec {
                  - active_deadline_seconds          = 0 -> null
                  + hostname                         = (known after apply)
                  + node_name                        = (known after apply)
                  - node_selector                    = {} -> null
                  ~ scheduler_name                   = "default-scheduler" -> (known after apply)
                  + service_account_name             = (known after apply)
                    # (9 unchanged attributes hidden)

                  ~ container {
                      - args                       = [] -> null
                      - command                    = [] -> null
                      ~ image_pull_policy          = "Always" -> (known after apply)
                        name                       = "bigquery-emulator"
                      ~ termination_message_policy = "File" -> (known after apply)
                        # (5 unchanged attributes hidden)

                      ~ port {
                          - host_port      = 0 -> null
                            # (2 unchanged attributes hidden)
                        }

                      - resources {
                          - limits   = {} -> null
                          - requests = {} -> null
                        }
                    }
                }
            }

            # (1 unchanged block hidden)
        }
    }

Plan: 1 to add, 0 to change, 1 to destroy.

Do you want to perform these actions?
  Terraform will perform the actions described above.
  Only 'yes' will be accepted to approve.

  Enter a value: yes

kubernetes_deployment.example: Destroying... [id=de-zoomcamp-namespace-week1/bigquery-emulator]
kubernetes_deployment.example: Destruction complete after 0s
kubernetes_deployment.example: Creating...
```
## Submitting the solutions

* Form for submitting: https://courses.datatalks.club/de-zoomcamp-2024/homework/hw01
* You can submit your homework multiple times. In this case, only the last submission will be used. 

Deadline: 29 January, 23:00 CET