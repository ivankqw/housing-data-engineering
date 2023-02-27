# Airflow 

## Setup

**You only have to do this once!** 

1. Install [Docker Desktop](https://docs.docker.com/get-docker/) and run it 
2. cd to `/housing-data-engineering/airflow` (Which is this directory)
3. Create two folders `/logs` and `/plugins` within `/housing-data-engineering/airflow`
3. Copy `.env` to `/housing-data-engineering/airflow` (See GDrive) 
3. Run `docker-compose up airflow-init`, then `docker-compose build`, then `docker-compose up`

## Usage 
1. Run Docker Desktop
1. cd to `/housing-data-engineering/airflow` 
2. Run `docker-compose up`
3. Go to [localhost:8080](http://localhost:8080) and input airflow as username and password 

### Stopping Airflow Docker Container
- ctrl-c within terminal to stop the docker-compose process


