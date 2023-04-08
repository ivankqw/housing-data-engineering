from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
import extract.ura as ura
import extract.datagovsg as datagovsg
import extract.singstat as singstat
import transform.transform as transform
from transform.transform_for_forecast import (
    transform_resale_transactions_ml,
    transform_flat_rental_ml,
    transform_private_transactions_ml,
    transform_private_rental_ml,
)
import os
import load.queries as queries
import pandas as pd
import pickle

data_path = "/opt/airflow/dags/data/"

with DAG(
    dag_id="housing_pipeline",
    default_args={"owner": "airflow"},
    schedule_interval=None,
    start_date=days_ago(2),
    tags=["is3107"],
) as dag:

    def extract_singstat_data(**kwargs):
        print("Getting Singstat data...")
        data_path = "/opt/airflow/dags/data"
        df_cpi = singstat.get_cpi()
        data_path_cpi = data_path + "/cpi.csv"
        df_cpi.to_csv(data_path_cpi, index=False)
        print("Singstat data obtained.")
        # push to task instance
        ti = kwargs["ti"]
        ti.xcom_push("df_cpi", data_path_cpi)

    def extract_ura_data(**kwargs):
        print("Getting URA data...")
        (
            df_private_transactions,
            df_private_rental,
            df_planning_decisions,
        ) = ura.get_all_ura()
        print("URA data obtained.")

        data_path = "/opt/airflow/dags/data"
        data_path_private_transactions = data_path + "/private_transactions.csv"
        data_path_private_rental = data_path + "/private_rental.csv"
        data_path_planning_decisions = data_path + "/planning_decisions.csv"

        # save to csv
        print("Saving to csv...")
        df_private_transactions.to_csv(data_path_private_transactions, index=False)
        df_private_rental.to_csv(data_path_private_rental, index=False)
        df_planning_decisions.to_csv(data_path_planning_decisions, index=False)
        print("Saved to csv.")
        # push to task instance
        ti = kwargs["ti"]
        ti.xcom_push("df_private_transactions", data_path_private_transactions)
        ti.xcom_push("df_private_rental", data_path_private_rental)
        ti.xcom_push("df_planning_decisions", data_path_planning_decisions)

        print("done")

    def extract_datagovsg_data(**kwargs):
        print("Getting resale flat transactions...")
        df_resale_flat_transactions = datagovsg.get_resale_flat_transactions()
        print("Getting salesperson information...")
        df_salesperson_info = datagovsg.get_salesperson_information()
        print("Getting salesperson transactions...")
        df_salesperson_trans = datagovsg.get_salesperson_transactions()
        print("Getting renting out of flats...")
        df_flat_rental = datagovsg.get_renting_out_of_flats_2023()
        print("Getting HDB property information...")
        df_hdb_information = datagovsg.get_hdb_property_information()
        # rename columns in df_hdb_information
        df_hdb_information = df_hdb_information.rename(
            columns={
                "1room_sold": "one_room_sold",
                "2room_sold": "two_room_sold",
                "3room_sold": "three_room_sold",
                "4room_sold": "four_room_sold",
                "5room_sold": "five_room_sold",
                "1room_rental": "one_room_rental",
                "2room_rental": "two_room_rental",
                "3room_rental": "three_room_rental",
            }
        )

        data_path = "/opt/airflow/dags/data"
        data_path_resale_flat_transactions = data_path + "/resale_flat_transactions.csv"
        data_path_salesperson_info = data_path + "/salesperson_info.csv"
        data_path_salesperson_trans = data_path + "/salesperson_transactions.csv"
        data_path_flat_rental = data_path + "/flat_rental.csv"
        data_path_hdb_information = data_path + "/hdb_information.csv"

        # save to csv
        df_resale_flat_transactions.to_csv(
            data_path_resale_flat_transactions, index=False
        )
        df_salesperson_info.to_csv(data_path_salesperson_info, index=False)
        df_salesperson_trans.to_csv(data_path_salesperson_trans, index=False)
        df_flat_rental.to_csv(data_path_flat_rental, index=False)
        df_hdb_information.to_csv(data_path_hdb_information, index=False)

        # push to task instance
        ti = kwargs["ti"]
        ti.xcom_push("df_resale_flat_transactions", data_path_resale_flat_transactions)
        ti.xcom_push("df_salesperson_info", data_path_salesperson_info)
        ti.xcom_push("df_salesperson_trans", data_path_salesperson_trans)
        ti.xcom_push("df_flat_rental", data_path_flat_rental)
        ti.xcom_push("df_hdb_information", data_path_hdb_information)

    def transform_resale_flat_transactions(**kwargs):
        print("Transforming resale flat transactions...")
        ti = kwargs["ti"]
        df_resale_flat_transactions_filename = ti.xcom_pull(
            task_ids="extract_datagovsg_data", key="df_resale_flat_transactions"
        )

        # transform
        df_districts = transform.read_and_transform_districts()  # type: ignore
        df_resale_flats = transform.transform_resale_flats(  # type: ignore
            df_resale_flat_transactions_filename, df_districts
        )

        # save to csv
        data_path = "/opt/airflow/dags/data"
        data_path_resale_flats = data_path + "/resale_flats_transformed.csv"
        data_path_districts = data_path + "/districts_transformed.csv"

        df_resale_flats.to_csv(data_path_resale_flats, index=False)

        df_districts = (
            df_districts.groupby("Postal District")
            .agg({"Postal Sector": lambda x: list(x)})
            .reset_index()
        )

        df_districts.to_csv(data_path_districts, index=False)

        # push to task instance
        ti.xcom_push("df_resale_flats", data_path_resale_flats)
        ti.xcom_push("df_districts", data_path_districts)

    def transform_private_transactions_and_rental(**kwargs):
        ti = kwargs["ti"]
        # get all the data from task instance
        df_private_transactions_filename = ti.xcom_pull(
            task_ids="extract_ura_data", key="df_private_transactions"
        )
        df_private_rental_filename = ti.xcom_pull(
            task_ids="extract_ura_data", key="df_private_rental"
        )

        print("Transforming private transactions and rental...")
        (
            df_private_transactions,
            df_private_rental,
        ) = transform.transform_private_transactions_and_rental(  # type: ignore
            df_private_transactions_filename, df_private_rental_filename
        )

        data_path = "/opt/airflow/dags/data"
        data_path_private_transactions = (
            data_path + "/private_transactions_transformed.csv"
        )
        data_path_private_rental = data_path + "/private_rental_transformed.csv"

        df_private_transactions.to_csv(data_path_private_transactions, index=False)
        df_private_rental.to_csv(data_path_private_rental, index=False)

        ti.xcom_push(
            "df_private_transactions_transformed", data_path_private_transactions
        )
        ti.xcom_push("df_private_rental_transformed", data_path_private_rental)

    def transform_salesperson_transactions(**kwargs):
        ti = kwargs["ti"]
        df_salesperson_trans_filename = ti.xcom_pull(
            task_ids="extract_datagovsg_data", key="df_salesperson_trans"
        )
        df_salesperson_info_filename = ti.xcom_pull(
            task_ids="extract_datagovsg_data", key="df_salesperson_info"
        )

        print("Transforming salesperson_transactions...")
        df_salesperson_transactions = transform.transform_salesperson_transactions(  # type: ignore
            df_salesperson_trans_filename, df_salesperson_info_filename
        )

        data_path = "/opt/airflow/dags/data"
        data_path_salesperson_transactions = (
            data_path + "/salesperson_transactions_transformed.csv"
        )

        df_salesperson_transactions.to_csv(
            data_path_salesperson_transactions, index=False
        )

        ti.xcom_push(
            "df_salesperson_transactions_transformed",
            data_path_salesperson_transactions,
        )

    def transform_rental_flats(**kwargs):
        ti = kwargs["ti"]
        df_flat_rental_filename = ti.xcom_pull(
            task_ids="extract_datagovsg_data", key="df_flat_rental"
        )

        print("Transforming rental flats...")
        df_rental_flats = transform.transform_rental_flats(  # type: ignore
            df_flat_rental_filename, transform.read_and_transform_districts()  # type: ignore
        )

        data_path = "/opt/airflow/dags/data"
        data_path_rental_flats = data_path + "/rental_flats_transformed.csv"

        df_rental_flats.to_csv(data_path_rental_flats, index=False)

        ti.xcom_push("df_rental_flats_transformed", data_path_rental_flats)

    def insert_cpi(**kwargs):
        ti = kwargs["ti"]
        df_cpi_filename = ti.xcom_pull(task_ids="extract_singstat_data", key="df_cpi")
        PostgresHook(postgres_conn_id="db_localhost").copy_expert(
            sql=queries.INSERT_CPI, filename=df_cpi_filename
        )

    def insert_resale_flats(**kwargs):
        ti = kwargs["ti"]
        df_resale_flats_filename = ti.xcom_pull(
            task_ids="transform_resale_flat_transactions", key="df_resale_flats"
        )
        PostgresHook(postgres_conn_id="db_localhost").copy_expert(
            sql=queries.INSERT_RESALE_FLATS, filename=df_resale_flats_filename
        )

    def insert_districts(**kwargs):
        ti = kwargs["ti"]
        df_districts_filename = ti.xcom_pull(
            task_ids="transform_resale_flat_transactions", key="df_districts"
        )
        PostgresHook(postgres_conn_id="db_localhost").copy_expert(
            sql=queries.INSERT_DISTRICTS, filename=df_districts_filename
        )

    def insert_salesperson_information(**kwargs):
        ti = kwargs["ti"]
        df_salesperson_info_filename = ti.xcom_pull(
            task_ids="extract_datagovsg_data", key="df_salesperson_info"
        )
        PostgresHook(postgres_conn_id="db_localhost").copy_expert(
            sql=queries.INSERT_SALESPERSON_INFORMATION,
            filename=df_salesperson_info_filename,
        )

    def insert_salesperson_transactions(**kwargs):
        ti = kwargs["ti"]
        df_salesperson_transactions_filename = ti.xcom_pull(
            task_ids="transform_salesperson_transactions",
            key="df_salesperson_transactions_transformed",
        )
        PostgresHook(postgres_conn_id="db_localhost").copy_expert(
            sql=queries.INSERT_SALESPERSON_TRANSACTIONS,
            filename=df_salesperson_transactions_filename,
        )

    def insert_private_transactions(**kwargs):
        ti = kwargs["ti"]
        df_private_transactions_filename = ti.xcom_pull(
            task_ids="transform_private_transactions_and_rental",
            key="df_private_transactions_transformed",
        )
        PostgresHook(postgres_conn_id="db_localhost").copy_expert(
            sql=queries.INSERT_PRIVATE_TRANSACTIONS,
            filename=df_private_transactions_filename,
        )

    def insert_private_rental(**kwargs):
        ti = kwargs["ti"]
        df_private_rental_filename = ti.xcom_pull(
            task_ids="transform_private_transactions_and_rental",
            key="df_private_rental_transformed",
        )
        PostgresHook(postgres_conn_id="db_localhost").copy_expert(
            sql=queries.INSERT_PRIVATE_RENTAL, filename=df_private_rental_filename
        )

    def insert_hdb_information(**kwargs):
        ti = kwargs["ti"]
        df_hdb_info_filename = ti.xcom_pull(
            task_ids="extract_datagovsg_data", key="df_hdb_information"
        )
        PostgresHook(postgres_conn_id="db_localhost").copy_expert(
            sql=queries.INSERT_HDB_INFORMATION, filename=df_hdb_info_filename
        )

    def insert_rental_flats(**kwargs):
        ti = kwargs["ti"]
        df_rental_flats_filename = ti.xcom_pull(
            task_ids="transform_rental_flats", key="df_rental_flats_transformed"
        )
        PostgresHook(postgres_conn_id="db_localhost").copy_expert(
            sql=queries.INSERT_RENTAL_FLATS, filename=df_rental_flats_filename
        )

    # todo
    def get_all_cpi(**kwargs):
        ti = kwargs["ti"]
        postgres = PostgresHook(postgres_conn_id="db_localhost")
        df_cpi = postgres.get_pandas_df("SELECT * FROM cpi;")
        cpi_path = "/opt/airflow/dags/data/cpi.csv"
        df_cpi.to_csv(cpi_path, index=False)
        ti.xcom_push("df_cpi", cpi_path)

    def get_five_year_resale_transactions(**kwargs):
        ti = kwargs["ti"]
        postgres = PostgresHook(postgres_conn_id="db_localhost")
        df_resale_flats = postgres.get_pandas_df(
            "SELECT * FROM resale_flats WHERE year >= 2018;"
        )
        resale_flats_path = "/opt/airflow/dags/data/resale_flats_transformed.csv"
        df_resale_flats.to_csv(resale_flats_path, index=False)
        ti.xcom_push("df_resale_flats", resale_flats_path)

    def get_flat_rental_transactions(**kwargs):
        ti = kwargs["ti"]
        postgres = PostgresHook(postgres_conn_id="db_localhost")
        df_rental_flats = postgres.get_pandas_df("SELECT * FROM rental_flats;")
        rental_flats_path = "/opt/airflow/dags/data/rental_flats_transformed.csv"
        df_rental_flats.to_csv(rental_flats_path, index=False)
        ti.xcom_push("df_rental_flats", rental_flats_path)

    def get_private_transactions(**kwargs):
        ti = kwargs["ti"]
        postgres = PostgresHook(postgres_conn_id="db_localhost")
        df_private_transactions = postgres.get_pandas_df(
            "SELECT * FROM private_transactions;"
        )
        private_transactions_path = (
            "/opt/airflow/dags/data/private_transactions_transformed.csv"
        )
        df_private_transactions.to_csv(private_transactions_path, index=False)
        ti.xcom_push("df_private_transactions", private_transactions_path)

    def get_private_rental(**kwargs):
        ti = kwargs["ti"]
        postgres = PostgresHook(postgres_conn_id="db_localhost")
        df_private_rental = postgres.get_pandas_df("SELECT * FROM private_rental;")
        private_rental_path = "/opt/airflow/dags/data/private_rental_transformed.csv"
        df_private_rental.to_csv(private_rental_path, index=False)
        ti.xcom_push("df_private_rental", private_rental_path)

    def get_hdb_information(**kwargs):
        ti = kwargs["ti"]
        postgres = PostgresHook(postgres_conn_id="db_localhost")
        df_hdb_info = postgres.get_pandas_df("SELECT * FROM hdb_information;")
        hdb_info_path = "/opt/airflow/dags/data/hdb_information.csv"
        df_hdb_info.to_csv(hdb_info_path, index=False)
        ti.xcom_push("df_hdb_info", hdb_info_path)

    def transform_resale_flat_transactions_ml(**kwargs):
        ti = kwargs["ti"]
        df_resale_flats_filename = ti.xcom_pull(
            task_ids="get_five_year_resale_transactions", key="df_resale_flats"
        )
        cpi_path = ti.xcom_pull(task_ids="get_all_cpi", key="df_cpi")
        hdb_info_path = ti.xcom_pull(task_ids="get_hdb_information", key="df_hdb_info")
        resale_flat_transactions_df_grouped_dict = (
            transform_resale_flat_transactions_ml(
                cpi_path,  # type: ignore
                df_resale_flats_filename,
                hdb_info_path,
            )
        )
        output_path = data_path + "resale_flat_transactions_df_grouped_dict.pkl"
        with open(output_path, "wb") as f:
            pickle.dump(resale_flat_transactions_df_grouped_dict, f)
        ti.xcom_push(
            "resale_flat_transactions_df_grouped_dict",
            output_path,
        )

    def transform_flat_rental_ml(**kwargs):
        ti = kwargs["ti"]
        df_rental_flats_filename = ti.xcom_pull(
            task_ids="get_flat_rental_transactions", key="df_rental_flats"
        )
        cpi_path = ti.xcom_pull(task_ids="get_all_cpi", key="df_cpi")
        rental_flat_df_grouped_dict = transform_flat_rental_ml(
            cpi_path, df_rental_flats_filename  # type: ignore
        )
        output_path = data_path + "rental_flat_df_grouped_dict.pkl"
        with open(output_path, "wb") as f:
            pickle.dump(rental_flat_df_grouped_dict, f)
        ti.xcom_push("rental_flat_df_grouped_dict", output_path)

    def transform_private_transactions_ml(**kwargs):
        ti = kwargs["ti"]
        df_private_transactions_filename = ti.xcom_pull(
            task_ids="get_private_transactions", key="df_private_transactions"
        )
        cpi_path = ti.xcom_pull(task_ids="get_all_cpi", key="df_cpi")
        private_transactions_df_grouped_dict = transform_private_transactions_ml(
            cpi_path, df_private_transactions_filename  # type: ignore
        )
        output_path = data_path + "private_transactions_df_grouped_dict.pkl"
        with open(output_path, "wb") as f:
            pickle.dump(private_transactions_df_grouped_dict, f)
        ti.xcom_push("private_transactions_df_grouped_dict", output_path)

    def transform_private_rental_ml(**kwargs):
        ti = kwargs["ti"]
        df_private_rental_filename = ti.xcom_pull(
            task_ids="get_private_rental", key="df_private_rental"
        )
        cpi_path = ti.xcom_pull(task_ids="get_all_cpi", key="df_cpi")
        private_rental_df_grouped_dict = transform_private_rental_ml(
            cpi_path, df_private_rental_filename  # type: ignore
        )
        output_path = data_path + "private_rental_df_grouped_dict.pkl"
        with open(output_path, "wb") as f:
            pickle.dump(private_rental_df_grouped_dict, f)
        ti.xcom_push("private_rental_df_grouped_dict", output_path)

    extract_singstat_data_task = PythonOperator(
        task_id="extract_singstat_data",
        python_callable=extract_singstat_data,
    )

    extract_ura_data_task = PythonOperator(
        task_id="extract_ura_data",
        python_callable=extract_ura_data,
    )

    extract_datagovsg_data_task = PythonOperator(
        task_id="extract_datagovsg_data",
        python_callable=extract_datagovsg_data,
    )

    transform_private_transactions_and_rental_task = PythonOperator(
        task_id="transform_private_transactions_and_rental",
        python_callable=transform_private_transactions_and_rental,
    )

    transform_resale_flat_transactions_task = PythonOperator(
        task_id="transform_resale_flat_transactions",
        python_callable=transform_resale_flat_transactions,
    )

    transform_salesperson_transactions_task = PythonOperator(
        task_id="transform_salesperson_transactions",
        python_callable=transform_salesperson_transactions,
    )

    transform_rental_flat_task = PythonOperator(
        task_id="transform_rental_flats",
        python_callable=transform_rental_flats,
    )

    get_all_cpi_task = PythonOperator(
        task_id="get_all_cpi",
        python_callable=get_all_cpi,
    )

    get_five_year_resale_transactions_task = PythonOperator(
        task_id="get_five_year_resale_transactions",
        python_callable=get_five_year_resale_transactions,
    )

    get_flat_rental_transactions_task = PythonOperator(
        task_id="get_flat_rental_transactions",
        python_callable=get_flat_rental_transactions,
    )

    get_private_transactions_task = PythonOperator(
        task_id="get_private_transactions",
        python_callable=get_private_transactions,
    )

    get_private_rental_task = PythonOperator(
        task_id="get_private_rental",
        python_callable=get_private_rental,
    )

    get_hdb_information_task = PythonOperator(
        task_id="get_hdb_information",
        python_callable=get_hdb_information,
    )

    transform_resale_flat_transactions_ml_task = PythonOperator(
        task_id="transform_resale_flat_transactions_ml",
        python_callable=transform_resale_flat_transactions_ml,
    )

    transform_flat_rental_ml_task = PythonOperator(
        task_id="transform_flat_rental_ml",
        python_callable=transform_flat_rental_ml,
    )

    transform_private_transactions_ml_task = PythonOperator(
        task_id="transform_private_transactions_ml",
        python_callable=transform_private_transactions_ml,
    )

    transform_private_rental_ml_task = PythonOperator(
        task_id="transform_private_rental_ml",
        python_callable=transform_private_rental_ml,
    )

    # create a postgres operator to execute the create_tables_query
    create_tables = PostgresOperator(
        task_id="create_tables",
        postgres_conn_id="db_localhost",
        sql=queries.CREATE_TABLES,
    )

    insert_cpi = PythonOperator(
        task_id="insert_cpi",
        python_callable=insert_cpi,
    )  # type: ignore

    insert_salesperson_information = PythonOperator(
        task_id="insert_salesperson_information",
        python_callable=insert_salesperson_information,
    )  # type: ignore

    insert_salesperson_transactions = PythonOperator(
        task_id="insert_salesperson_transactions",
        python_callable=insert_salesperson_transactions,
    )  # type: ignore

    insert_districts = PythonOperator(
        task_id="insert_districts",
        python_callable=insert_districts,
    )  # type: ignore

    insert_private_transactions = PythonOperator(
        task_id="insert_private_transactions",
        python_callable=insert_private_transactions,
    )  # type: ignore

    insert_private_rental = PythonOperator(
        task_id="insert_private_rental",
        python_callable=insert_private_rental,
    )  # type: ignore

    insert_hdb_information = PythonOperator(
        task_id="insert_hdb_information",
        python_callable=insert_hdb_information,
    )  # type: ignore

    insert_resale_flats = PythonOperator(
        task_id="insert_resale_flats",
        python_callable=insert_resale_flats,
    )  # type: ignore

    insert_rental_flats = PythonOperator(
        task_id="insert_rental_flats",
        python_callable=insert_rental_flats,
    )  # type: ignore

    # create a postgres operator to execute the create_tables_query
    alter_tables = PostgresOperator(
        task_id="alter_tables",
        postgres_conn_id="db_localhost",
        sql=queries.ALTER_TABLES,
    )

    extract_ura_data_task >> transform_private_transactions_and_rental_task

    extract_datagovsg_data_task >> [
        transform_resale_flat_transactions_task,
        transform_salesperson_transactions_task,
        transform_rental_flat_task,
    ]
    
    (
        [
            extract_singstat_data_task,
            transform_rental_flat_task,
            transform_private_transactions_and_rental_task,
            transform_resale_flat_transactions_task,
            transform_salesperson_transactions_task,
        ]
        >> create_tables
        >> [
            insert_cpi,
            insert_salesperson_information,
            insert_salesperson_transactions,
            insert_districts,
            insert_private_transactions,
            insert_private_rental,
            insert_hdb_information,
            insert_resale_flats,
            insert_rental_flats,
        ]  # type: ignore
        >> alter_tables
        >> [
            get_all_cpi_task,
            get_five_year_resale_transactions_task,
            get_flat_rental_transactions_task,
            get_private_transactions_task,
            get_private_rental_task,
            get_hdb_information_task,
        ]
    )

    (
        [
            get_all_cpi_task,
            get_five_year_resale_transactions_task,
            get_hdb_information_task,
        ]
        >> transform_resale_flat_transactions_ml_task,
    )

    (
        [
            get_all_cpi_task,
            get_flat_rental_transactions_task,
        ]
        >> transform_flat_rental_ml_task,
    )

    (
        [
            get_all_cpi_task,
            get_private_transactions_task,
        ]
        >> transform_private_transactions_ml_task,
    )

    (
        [  
            get_all_cpi_task,
            get_private_rental_task,
        ]
        >> transform_private_rental_ml_task,
    )


