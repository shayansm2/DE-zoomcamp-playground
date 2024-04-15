# GitHub Events

![schema.png](files/overview.png)
description

## Used Technologies

- DataBase: **Elasticsearch**
- Streaming System: **Redpanda**
- Dashboard: **Kibana**
- Monitoring: **Redpanda console** and **Elasticvue**

## Evaluation Criteria

### Problem description

Project Structure:
![schema.png](files/project_schema.png)

## Cloud

### Data ingestion (Stream)
![img.png](files/redpanda.png)

### DataBase
![img.png](files/elasticvue.png)
### Transformations

### Dashboard

![discover.jpeg](files/discover.jpeg)
![dashboard.png](files/dashboard.png)

- dashboard url: https://de-zoomcamp-project2-dashboard.darkube.app
- username: `visitor`
- password: `visitorpassword`

add your analysis and insights and GitHub yearly report here.

### Reproducibility

how to build:

1. clone this repo using the below command

```bash
git clone 
```

2. build the project
   go to the project folder

```bash
cd tech-career-explorer
```

add execution access to build file

```bash
docker 
```

have in mind that you should have a running docker in your system for building this project.

3. now you can check the exported ports

| link                                                                   | purpose                               |
|------------------------------------------------------------------------|---------------------------------------|
| [mage ai panel](http://localhost:6789/)                                | for checking and running pipelines    |
| [dbt docs](http://localhost:8080/)                                     | for checking dbt docs and lineage     |
| [metaase dahsboard](http://localhost:3000/dashboard/1-job-positions)   | for checking the dashboards           |
| localhost:5432                                                         | connecting to the postgresql database |
| localhost:5432                                                         | connecting to the postgresql database |
| localhost:5432                                                         | connecting to the postgresql database |