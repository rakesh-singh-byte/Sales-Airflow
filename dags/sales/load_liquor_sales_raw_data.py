"""
DAG for Liquor Sales Data Pipeline

This Airflow DAG handles the process of downloading liquor sales raw data from an S3 bucket,
ingesting it into a PostgreSQL database, and cleaning up local files. It also sends email notifications
if any task in the pipeline fails.
"""

import os

from airflow import DAG
from airflow.models import Variable
from airflow.operators.email import EmailOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from utils.utils import (
    check_and_create_table,
    clean_local_files,
    download_files_from_s3,
    ingest_files_to_postgres,
    truncate_table,
)
from utils.variables import load_environment_variables

# Load environment-specific variables
env_vars = load_environment_variables()

# Set variables from Airflow Variables or environment-specific defaults
email_recipient = Variable.get("EMAIL_RECIPIENT")
failure_email_subject = Variable.get("FAILURE_EMAIL_SUBJECT")
failure_email_content = Variable.get("FAILURE_EMAIL_CONTENT")
dbt_directory = Variable.get(f"DBT_DIR_{env_vars['env']}")
project_name = Variable.get("SALES_PROJECT_NAME")
sales_bucket_name = Variable.get("SALES_BUCKET_NAME")
file_prefix = Variable.get("LIQUOR_SALES_FILE_PREFIX")
liquor_sales_raw_data_table_name = Variable.get(
    "LIQUOR_SALES_RAW_DATA_TABLE_NAME"
)
liquor_sales_raw_data_table_sql = Variable.get("LIQUOR_SALES_TABLE_SQL")
liquor_sales_database = Variable.get("LIQUOR_SALES_DATABASE")
data_directory = os.path.join(dbt_directory, 'data')

# Ensure the data directory exists
os.makedirs(data_directory, exist_ok=True)

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 0  # No retries if a task fails
}

# Define the DAG
dag = DAG(
    'liquor_sales_data_pipeline',
    default_args=default_args,
    description=(
        'Pipeline to download liquor sales raw data from S3, '
        'ingest into Postgres, and cleanup local files.'
    ),
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,  # Do not backfill runs
    # Tagging the DAG for easier filtering
    tags=["liquor_sales", env_vars["env"]]
)

# Task to check if the table exists and create it if it doesn't
check_create_table = PythonOperator(
    task_id='check_create_table',
    python_callable=check_and_create_table,
    op_kwargs={
        'database_name': liquor_sales_database,
        'table_name': liquor_sales_raw_data_table_name,
        'postgres_conn_id': env_vars['postgres_conn_id'],
        'create_table_sql': liquor_sales_raw_data_table_sql
    },
    trigger_rule='always',  # This task should always run
    dag=dag,
)

# Task to download liquor sales data from S3
download_sales_data = PythonOperator(
    task_id='download_liquor_sales_data_from_s3',
    python_callable=download_files_from_s3,
    op_kwargs={
        'bucket_name': sales_bucket_name,
        'file_prefix': file_prefix,
        'local_path_dir': data_directory,
        'aws_conn_id': env_vars['aws_conn_id']
    },
    trigger_rule='all_success',  # Only run if all upstream tasks are successful
    dag=dag,
)

# Task to truncate the target table before ingesting new data
truncate_target_table = PythonOperator(
    task_id='truncate_liquor_target_table',
    python_callable=truncate_table,
    op_kwargs={
        'database_name': liquor_sales_database,
        'table_name': liquor_sales_raw_data_table_name,
        'postgres_conn_id': env_vars['postgres_conn_id']
    },
    trigger_rule='all_success',  # Only run if all upstream tasks are successful
    dag=dag,
)

# Task to ingest the downloaded data into PostgreSQL
ingest_sales_data = PythonOperator(
    task_id='ingest_liquor_sales_data_into_postgres',
    python_callable=ingest_files_to_postgres,
    op_kwargs={
        'local_path_dir': data_directory,
        'file_prefix': file_prefix,
        'database_name': liquor_sales_database,
        'table_name': liquor_sales_raw_data_table_name,
        'postgres_conn_id': env_vars['postgres_conn_id']
    },
    trigger_rule='all_success',  # Only run if all upstream tasks are successful
    dag=dag,
)

# Task to clean up local files after ingestion
cleanup_local_files = PythonOperator(
    task_id='cleanup_liquor_local_files',
    python_callable=clean_local_files,
    op_kwargs={'local_path_dir': data_directory},
    trigger_rule='all_done',  # Run this task regardless of the previous task's outcome
    dag=dag,
)

# Task to send an email notification if any task fails
notify_failure_email = EmailOperator(
    task_id='notify_failure_email',
    to=[email_recipient],
    subject=failure_email_subject,
    html_content=failure_email_content.replace('{dag.dag_id}', dag.dag_id),
    trigger_rule='one_failed',  # Run this task only if any previous task fails
    dag=dag,
)

# Set task dependencies for the DAG workflow
# Ensure table exists before downloading data
check_create_table >> download_sales_data  # type: ignore
# Truncate the table after downloading data
download_sales_data >> truncate_target_table  # type: ignore
# Ingest data after truncating the table
truncate_target_table >> ingest_sales_data  # type: ignore
# Clean up local files after ingestion
ingest_sales_data >> cleanup_local_files  # type: ignore

# Set failure notification dependencies
# Notify if table creation/check fails
check_create_table >> notify_failure_email  # type: ignore
# Notify if download fails
download_sales_data >> notify_failure_email  # type: ignore
# Notify if truncation fails
truncate_target_table >> notify_failure_email  # type: ignore
# Notify if ingestion fails
ingest_sales_data >> notify_failure_email  # type: ignore
# Notify if cleanup fails
cleanup_local_files >> notify_failure_email  # type: ignore
