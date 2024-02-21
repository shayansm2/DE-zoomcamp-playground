#!/bin/bash

if [ ! -f ".env" ]; then
    touch .env
    echo PROJECT_NAME=de_zoomcamp_week4_hw >> .env
    echo POSTGRES_DBNAME=postgres >> .env
    echo POSTGRES_SCHEMA=public >> .env
    echo POSTGRES_USER=user >> .env
    echo POSTGRES_PASSWORD=password >> .env
    echo POSTGRES_HOST=de_zoomcamp_week4_hw_postgres >> .env
    echo POSTGRES_PORT=5432 >> .env
fi

docker-compose up --build --name de-zoomcamp-hw4