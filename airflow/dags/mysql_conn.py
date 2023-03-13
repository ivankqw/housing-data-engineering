from airflow.models import Connection
from airflow.models import Variable
from airflow.utils.db import create_session
from airflow import settings

conn_id = '10'
conn_type = 'mysql'
host = 'localhost'
port = 3306
login = 'user'
password = 'password'
schema = 'db'

# Create a Connection object
conn = Connection(
    conn_id=conn_id,
    conn_type=conn_type,
    host=host,
    port=port,
    login=login,
    password=password,
    schema=schema
)

# Add the Connection to the metadata database
with create_session() as session:
    session.add(conn)
    session.commit()

# Set the default MySQL connection in Airflow
Variable.set('default_mysql_conn', conn_id)
