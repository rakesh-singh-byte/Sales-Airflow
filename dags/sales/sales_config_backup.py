"""
sales_config_backup.py

This module defines an Apache Airflow DAG for exporting Airflow variables and connections to JSON files,
uploading them to an S3 bucket, and cleaning up local files. It also includes error handling and notifications.

Key Components:
- `export_variables()`: Exports Airflow variables to a JSON file and uploads it to S3.
- `export_connections()`: Exports Airflow connections to a JSON file and uploads it to S3.
- `cleanup_local_files_task`: Cleans up local files after processing.
- `notify_failure_email_task`: Sends an email notification if any task fails.

Configuration:
- Airflow Variables:
  - `SALES_CONFIG_BUCKET_NAME`: S3 bucket name for backup
  - `AIRFLOW_DIR`: Directory path for Airflow data
  - `EMAIL_RECIPIENT`: Recipient email address for failure notifications
  - `FAILURE_EMAIL_SUBJECT`: Subject line for failure notifications
  - `FAILURE_EMAIL_CONTENT`: HTML content for failure notifications

Dependencies:
- Requires `utils.utils` module for file operations and S3 uploads.

Usage:
To use this script, ensure that all required Airflow variables are set. The DAG can be triggered manually.
"""

import json
import os
from datetime import datetime

from airflow import DAG
from airflow.models import Connection, Variable
from airflow.operators.email import EmailOperator
from airflow.operators.python import PythonOperator
from airflow.utils.session import create_session
from utils.utils import clean_local_files, upload_files_to_s3
from utils.variables import load_environment_variables

# Load environment-specific variables
env_vars = load_environment_variables()

# Configuration
bucket_name = Variable.get("SALES_CONFIG_BUCKET_NAME")
airflow_directory = Variable.get(f"AIRFLOW_DIR_{env_vars['env']}")
data_directory = os.path.join(airflow_directory, 'data')
email_recipient = Variable.get("EMAIL_RECIPIENT")
failure_email_subject = Variable.get("FAILURE_EMAIL_SUBJECT")
failure_email_content = Variable.get("FAILURE_EMAIL_CONTENT")

# Ensure the data directory exists
os.makedirs(data_directory, exist_ok=True)

# File paths for the JSON exports
variables_file_path = os.path.join(data_directory, 'variables.json')
connections_file_path = os.path.join(data_directory, 'connections.json')


def export_variables() -> None:
    """
    Export Airflow variables to a JSON file and upload to S3.
    """
    with create_session() as session:
        # Query all Airflow variables
        variables = session.query(Variable).all()
        # Convert the variables into a dictionary
        variables_dict = {var.key: var.val for var in variables}
        # Write the dictionary to a JSON file
        with open(variables_file_path, 'w', encoding="utf-8") as outfile:
            json.dump(variables_dict, outfile, indent=4)
    # Upload the JSON file to S3
    upload_files_to_s3(bucket_name, variables_file_path,
                       env_vars['aws_conn_id'])


def export_connections() -> None:
    """
    Export Airflow connections to a JSON file and upload to S3.
    """
    with create_session() as session:
        # Query all Airflow connections
        connections = session.query(Connection).all()
        # Convert the connections into a list of dictionaries
        connections_list = [
            {
                "conn_id": conn.conn_id,
                "conn_type": conn.conn_type,
                "host": conn.host,
                "schema": conn.schema,
                "login": conn.login,
                "password": conn.password,
                "port": conn.port,
                "extra": conn.extra,
            }
            for conn in connections
        ]
        # Write the list to a JSON file
        with open(connections_file_path, 'w', encoding="utf-8") as outfile:
            json.dump(connections_list, outfile, indent=4)
    # Upload the JSON file to S3
    upload_files_to_s3(bucket_name, connections_file_path,
                       env_vars['aws_conn_id'])


# Define the DAG
dag = DAG(
    'sales_config_backup',
    default_args=env_vars["default_args"],
    schedule_interval=None,  # Manual trigger
    start_date=datetime(2024, 1, 1),
    catchup=False,  # Prevents backfilling
    tags=['backup', env_vars["env"]]
)

# Task for exporting Airflow variables
export_variables_task = PythonOperator(
    task_id='export_variables',
    python_callable=export_variables,
    dag=dag,
)

# Task for exporting Airflow connections
export_connections_task = PythonOperator(
    task_id='export_connections',
    python_callable=export_connections,
    dag=dag,
)

# Task to clean up local files after processing
cleanup_local_files_task = PythonOperator(
    task_id='cleanup_local_files',
    python_callable=clean_local_files,
    op_kwargs={'local_path_dir': data_directory},
    # Ensures cleanup runs regardless of success or failure of previous tasks
    trigger_rule='all_done',
    dag=dag,
)

# Task to send an email notification if any task fails
notify_failure_email_task = EmailOperator(
    task_id='notify_failure_email',
    to=[email_recipient],
    subject=failure_email_subject,
    html_content=failure_email_content.replace('{dag.dag_id}', dag.dag_id),
    trigger_rule='one_failed',  # Trigger this task if any of the previous tasks fail
    dag=dag
)

# Set task dependencies
# Notify if exporting variables fails
export_variables_task >> notify_failure_email_task  # type: ignore
# Notify if exporting connections fails
export_connections_task >> notify_failure_email_task  # type: ignore
# Notify if cleanup fails
cleanup_local_files_task >> notify_failure_email_task   # type: ignore
