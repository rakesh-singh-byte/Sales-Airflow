import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.models import Variable, Connection
from airflow.settings import Session
import json


airflow_directory = Variable.get("AIRFLOW_DIR", default_var='/Users/rakeshsingh/Personal/airflow/')
data_directory = f'{airflow_directory}/data'

# Ensure the data directory exists
os.makedirs(data_directory, exist_ok=True)

# File paths
variables_file_path = f'{data_directory}/variables.json'
connections_file_path = f'{data_directory}/connections.json'


# Export Variables
def export_variables():
    session = Session()
    variables = session.query(Variable).all()
    variables_dict = {var.key: var.val for var in variables}
    with open(variables_file_path, 'w') as outfile:
        json.dump(variables_dict, outfile, indent=4)
    session.close()
    print(f"Variables exported to {variables_file_path}")


# Export Connections
def export_connections():
    session = Session()
    connections = session.query(Connection).all()
    connections_list = []
    for conn in connections:
        connections_list.append({
            "conn_id": conn.conn_id,
            "conn_type": conn.conn_type,
            "host": conn.host,
            "schema": conn.schema,
            "login": conn.login,
            "password": conn.password,
            "port": conn.port,
            "extra": conn.extra,
        })
    with open(connections_file_path, 'w') as outfile:
        json.dump(connections_list, outfile, indent=4)
    session.close()
    print(f"Connections exported to {connections_file_path}")


# Define the DAG
with DAG('sales_config_backup', schedule_interval=None, catchup=False) as dag:

    # Tasks for exporting
    export_variables_task = PythonOperator(
        task_id='export_variables',
        python_callable=export_variables
    )

    export_connections_task = PythonOperator(
        task_id='export_connections',
        python_callable=export_connections
    )



    # Task dependencies
    export_variables_task # type: ignore
    export_connections_task  # type: ignore