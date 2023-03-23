# Airflow 

## Setup

**You only have to do this once!** 

1. Install [Docker Desktop](https://docs.docker.com/get-docker/) and run it 
2. cd to `/housing-data-engineering/airflow` (Which is this directory)
3. Create two folders `/logs` and `/plugins` within `/housing-data-engineering/airflow`
4. Copy `.env` to `/housing-data-engineering/airflow` (See GDrive) 
5. Run `docker-compose up airflow-init`, then `docker-compose build`, then `docker-compose up`

## Usage 
1. Run Docker Desktop
2. cd to `/housing-data-engineering/airflow` 
3. Run `docker-compose up`
4. Go to [localhost:8080](http://localhost:8080) and input airflow as username and password 
5. Go to Admin > Connections, Create a new connection, enter the connection id (postgres_localhost), connection type (postgres), Host (airflow-db-1), Login and Password (db), Port (5432).
6. Run the dag `/dags/housing_pipeline.py`.

### Stopping Airflow Docker Container
- ctrl-c within terminal to stop the docker-compose process


