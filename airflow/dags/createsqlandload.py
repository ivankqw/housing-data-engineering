from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

with DAG(
    dag_id="create_sql_and_load",
    default_args={"owner": "airflow"},
    schedule_interval=None,
    start_date=days_ago(2),
    tags=["is3107"],
) as dag:

    create_tables_query = """
            CREATE TABLE IF NOT EXISTS resale_flats (
                town VARCHAR(255),
                flat_type VARCHAR(255),
                flat_model VARCHAR(255),
                floor_area_sqm VARCHAR(255),
                street_name VARCHAR(255),
                resale_price VARCHAR(255),
                month VARCHAR(255),
                remaining_lease VARCHAR(255),
                lease_commence_date VARCHAR(255),
                storey_range VARCHAR(255),
                id INT NOT NULL,
                block VARCHAR(255),
                year VARCHAR(255),
                street_name_with_block VARCHAR(255),
                postal VARCHAR(255),
                x_coord VARCHAR(255),
                y_coord VARCHAR(255),
                latitude VARCHAR(255),
                longitude VARCHAR(255),
                district VARCHAR(255)
            );
            
            CREATE TABLE IF NOT EXISTS private_transactions (
                area VARCHAR(255),
                floor_range VARCHAR(255),
                number_of_units VARCHAR(255),
                contract_date VARCHAR(255),
                type_of_sale VARCHAR(255),
                price VARCHAR(255),
                property_type VARCHAR(255),
                district VARCHAR(255),
                type_of_area VARCHAR(255),
                tenure VARCHAR(255),
                nett_price VARCHAR(255),
                street_name VARCHAR(255),
                project_name VARCHAR(255),
                market_segment VARCHAR(255),
                month VARCHAR(255),
                year VARCHAR(255)
            );
            
            CREATE TABLE IF NOT EXISTS private_rental (
                area_sqm VARCHAR(255),
                lease_date VARCHAR(255),
                property_type VARCHAR(255),
                district VARCHAR(255),
                area_sqft VARCHAR(255),
                number_of_bedrooms VARCHAR(255),
                rental VARCHAR(255),
                street_name VARCHAR(255),
                project_name VARCHAR(255),
                month VARCHAR(255),
                year VARCHAR(255)
            );
            """

    # COPY function not working

    insert_resale_flats_query = """
        COPY resale_flats FROM '/opt/airflow/dags/data/resale_flats_transformed.csv' DELIMITER ',' CSV HEADER;
        """

    insert_private_transactions_query = """
        COPY private_transactions FROM '/opt/airflow/dags/data/private_transactions_transformed.csv' DELIMITER ',' CSV HEADER;
        """

    insert_private_rental_query = """
        COPY private_rental FROM '/opt/airflow/dags/data/private_rental_transformed.csv' DELIMITER ',' CSV HEADER;
        """

    # create a postgres operator to execute the create_tables_query
    create_tables = PostgresOperator(
        task_id="create_tables",
        postgres_conn_id="db_localhost",
        sql=create_tables_query,
    )

    # create a postgres operator to execute the insert_resale_flats_query
    insert_resale_flats = PostgresOperator(
        task_id="insert_resale_flats",
        postgres_conn_id="db_localhost",
        sql=insert_resale_flats_query,
    )

    # create a postgres operator to execute the insert_private_transactions_query
    insert_private_transactions = PostgresOperator(
        task_id="insert_private_transactions",
        postgres_conn_id="db_localhost",
        sql=insert_private_transactions_query,
    )

    # create a postgres operator to execute the insert_private_rental_query
    insert_private_rental = PostgresOperator(
        task_id="insert_private_rental",
        postgres_conn_id="db_localhost",
        sql=insert_private_rental_query,
    )

    # set the order of execution
    create_tables >> insert_resale_flats >> insert_private_transactions >> insert_private_rental



