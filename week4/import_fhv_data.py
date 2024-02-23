import requests

url = 'http://localhost:6789/api/pipeline_schedules/1/pipeline_runs/dced12ac5aa84dcea4eaec764d91da86' 

for month in range(1, 13):
    data = {
        'pipeline_run': {
            'variables': {
                "url": f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-{month:02d}.csv.gz",
                "tbl_name": "fhv_tripdata"
            }
        }
    }

    response = requests.post(url, json=data)