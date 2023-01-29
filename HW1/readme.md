build pg_database: `docker-compose -f docker-compose.yaml up -d`
running python script: 
```commandline
URL="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz"
python ingest_data.py --user=root --password=root --host=localhost --port=5432 --db=ny_taxi --table_name=green_taxi_trips --url=$URL

URL="https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv"
python ingest_data.py --user=root --password=root --host=localhost --port=5432 --db=ny_taxi --table_name=taxi_zones --url=$URL
```
build py_pipeline: 
```commandline
todo
```