import json
import os
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.models import Variable, Connection
from airflow.utils.session import create_session

from utils.utils import upload_files_to_s3, clean_local_files

# Configuration
bucket_name = Variable.get("SALES_CONFIG_BUCKET_NAME", default_var='sales-config-backup')
airflow_directory = Variable.get("AIRFLOW_DIR", default_var='/Users/rakeshsingh/Personal/airflow/')
data_directory = os.path.join(airflow_directory, 'data')
email_recipient = Variable.get("EMAIL_RECIPIENT", default_var='rakesh.singh@tothenew.com')
failure_email_subject = Variable.get("FAILURE_EMAIL_SUBJECT", default_var='DAG Failure Notification')
failure_email_content = Variable.get(
    "FAILURE_EMAIL_CONTENT",
    default_var="""<h3>Dear Team,</h3><p>The DAG <strong>'sales_data_pipeline'</strong> failed.</p>"""
)

os.makedirs(data_directory, exist_ok=True)

variables_file_path = os.path.join(data_directory, 'variables.json')
connections_file_path = os.path.join(data_directory, 'connections.json')


def export_variables():
    """Export Airflow variables to a JSON file and upload to S3."""
    with create_session() as session:
        variables = session.query(Variable).all()
        variables_dict = {var.key: var.val for var in variables}
        with open(variables_file_path, 'w') as outfile:
            json.dump(variables_dict, outfile, indent=4)
    upload_files_to_s3(bucket_name, variables_file_path)


def export_connections():
    """Export Airflow connections to a JSON file and upload to S3."""
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
        with open(connections_file_path, 'w') as outfile:
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

# Task to cleanup the local data directory
cleanup_local_files_task = PythonOperator(
    task_id='cleanup_local_files',
    python_callable=clean_local_files,
    op_kwargs={'local_path_dir': data_directory},
    trigger_rule='all_done',
    dag=dag,
)

# Task to notify if pipeline or any tasks fail
notify_failure_email_task = EmailOperator(
    task_id='notify_failure_email',
    to=[email_recipient],
    subject=failure_email_subject,
    html_content=failure_email_content.replace('{dag.dag_id}', dag.dag_id),
    trigger_rule='one_failed',
    dag=dag,
)

# Set task dependencies
export_variables_task >> notify_failure_email_task # type: ignore
export_connections_task >> notify_failure_email_task # type: ignore
cleanup_local_files_task >> notify_failure_email_task  # type: ignore
