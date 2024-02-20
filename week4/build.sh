#!/bin/bash

if [ ! -f ".env" ]; then
    touch .env
    echo PROJECT_NAME=de-zoomcamp-week4-hw >> .env
    echo POSTGRES_DBNAME=postgres >> .env
    echo POSTGRES_SCHEMA=public >> .env
    echo POSTGRES_USER=user >> .env
    echo POSTGRES_PASSWORD=password >> .env
    echo POSTGRES_HOST=localhost >> .env
    echo POSTGRES_PORT=5432 >> .env
fi

docker-compose up --build