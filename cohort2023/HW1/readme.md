questions: [link](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/cohorts/2023/week_1_docker_sql/homework.md)

build pg_database: `docker-compose -f docker-compose.yaml up -d`

grant access for postgres tables
```commandline
docker exec -it hw1_pg_database_1 bash
psql -d ny_taxi root -W
```
```sql
GRANT ALL PRIVILEGES ON DATABASE ny_taxi TO root;
```

running python script: 
```commandline
URL="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz"
python ingest_data.py --user=root --password=root --host=localhost --port=5432 --db=ny_taxi --table_name= --url=$URL

URL="https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv"
python ingest_data.py --user=root --password=root --host=localhost --port=5432 --db=ny_taxi --table_name=taxi_zones --url=$URL
```
build py_pipeline: 
```commandline
todo
```