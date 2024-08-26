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
  - `SALES_CONFIG_BUCKET_NAME`: S3 bucket name for backup (default: 'sales-config-backup')
  - `AIRFLOW_DIR`: Directory path for Airflow data (default: '/Users/rakeshsingh/Personal/airflow/')
  - `EMAIL_RECIPIENT`: Recipient email address for failure notifications (default: 'rakesh.singh@tothenew.com')
  - `FAILURE_EMAIL_SUBJECT`: Subject line for failure notifications (default: 'DAG Failure Notification')
  - `FAILURE_EMAIL_CONTENT`: HTML content for failure notifications (default: "<h3>Dear Team,</h3><p>The DAG <strong>'sales_data_pipeline'</strong> failed.</p>")

Dependencies:
- Requires `utils.utils` module for file operations and S3 uploads.

Usage:
To use this script, ensure that all required Airflow variables are set. The DAG can be triggered manually.
"""

import json
import os
from datetime import datetime

from airflow.models import Connection, Variable
from airflow.operators.email import EmailOperator
from airflow.operators.python import PythonOperator
from utils.utils import clean_local_files, upload_files_to_s3

from airflow import DAG
from airflow.utils.session import create_session

# Configuration
bucket_name = Variable.get("SALES_CONFIG_BUCKET_NAME",
                           default_var='sales-config-backup')
airflow_directory = Variable.get(
    "AIRFLOW_DIR", default_var='/Users/rakeshsingh/Personal/airflow/')
data_directory = os.path.join(airflow_directory, 'data')
email_recipient = Variable.get(
    "EMAIL_RECIPIENT", default_var='rakesh.singh@tothenew.com')
failure_email_subject = Variable.get(
    "FAILURE_EMAIL_SUBJECT", default_var='DAG Failure Notification')
failure_email_content = Variable.get(
    "FAILURE_EMAIL_CONTENT",
    default_var="""<h3>Dear Team,</h3><p>The DAG <strong>'sales_data_pipeline'</strong> failed.</p>"""
)

os.makedirs(data_directory, exist_ok=True)

variables_file_path = os.path.join(data_directory, 'variables.json')
connections_file_path = os.path.join(data_directory, 'connections.json')


def export_variables() -> None:
    """
    Export Airflow variables to a JSON file and upload to S3.
    """
    with create_session() as session:
        variables = session.query(Variable).all()
        variables_dict = {var.key: var.val for var in variables}
        with open(variables_file_path, 'w', encoding="utf-8") as outfile:
            json.dump(variables_dict, outfile, indent=4)
    upload_files_to_s3(bucket_name, variables_file_path)


def export_connections() -> None:
    """
    Export Airflow connections to a JSON file and upload to S3.
    """
    with create_session() as session:
        connections = session.query(Connection).all()
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
        with open(connections_file_path, 'w', encoding="utf-8") as outfile:
            json.dump(connections_list, outfile, indent=4)
    upload_files_to_s3(bucket_name, connections_file_path)


# Define the DAG
dag = DAG(
    'sales_config_backup',
    schedule_interval=None,  # Manual trigger
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['backup']
)

# Tasks for exporting
export_variables_task = PythonOperator(
    task_id='export_variables',
    python_callable=export_variables,
    dag=dag,
)

export_connections_task = PythonOperator(
    task_id='export_connections',
    python_callable=export_connections,
    dag=dag,
)

# Task to clean up local files
cleanup_local_files_task = PythonOperator(
    task_id='cleanup_local_files',
    python_callable=clean_local_files,
    op_kwargs={'local_path_dir': data_directory},
    trigger_rule='all_done',
    dag=dag,
)

# Task to notify if any task fails
notify_failure_email_task = EmailOperator(
    task_id='notify_failure_email',
    to=[email_recipient],
    subject=failure_email_subject,
    html_content=failure_email_content.replace('{dag.dag_id}', dag.dag_id),
    trigger_rule='one_failed',
    dag=dag
)

# Set task dependencies
export_variables_task >> notify_failure_email_task  # type: ignore
export_connections_task >> notify_failure_email_task  # type: ignore
cleanup_local_files_task >> notify_failure_email_task  # type: ignore
