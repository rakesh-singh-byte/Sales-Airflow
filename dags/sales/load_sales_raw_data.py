"""
sales_data_pipeline.py

This module defines an Apache Airflow DAG for managing a sales data pipeline. The pipeline performs the following tasks:
1. Checks for the existence of a specified table in a PostgreSQL database and creates it if it does not exist.
2. Downloads raw sales data CSV files from an S3 bucket to a local directory.
3. Truncates the target table in the PostgreSQL database.
4. Ingests the downloaded CSV files into the PostgreSQL database.
5. Cleans up local files after ingestion.
6. Sends an email notification if any task fails.

The module retrieves configuration settings and connection details from Airflow variables and environment variables. 
It also includes functionality to handle errors by sending email notifications.

Key Components:
- `load_environment_variables()`: Retrieves AWS and PostgreSQL connection IDs from environment variables, with default values if not set.
- `check_create_table`: Checks if a PostgreSQL table exists and creates it if necessary.
- `download_sales_data`: Downloads CSV files from S3 to a local directory.
- `truncate_target_table`: Truncates the target table in PostgreSQL.
- `ingest_sales_data`: Ingests CSV data from the local directory into PostgreSQL.
- `cleanup_local_files`: Cleans up local files after processing.
- `notify_failure_email`: Sends an email notification if any task in the DAG fails.

Configuration:
- Environment Variables:
  - `AWS_CONN_ID`: AWS connection ID
  - `POSTGRES_CONN_ID`: PostgreSQL connection ID
- Airflow Variables:
  - `EMAIL_RECIPIENT`: Recipient email address for failure notifications.
  - `FAILURE_EMAIL_SUBJECT`: Subject line for failure notifications.
  - `FAILURE_EMAIL_CONTENT`: HTML content for failure notifications.
  - `DBT_DIR`: Directory path for DBT project.
  - `SALES_PROJECT_NAME`: Name of the sales project.
  - `SALES_BUCKET_NAME`: S3 bucket name for sales data.
  - `SALES_FILE_PREFIX`: Prefix for sales data files.
  - `SALES_RAW_DATA_TABLE_NAME`: Name of the PostgreSQL table for raw sales data.
  - `SALES_TABLE_SQL`: SQL query to create the sales table.
  - `SALES_DATABASE`: Name of the PostgreSQL database.

Dependencies:
- Requires `utils.utils` module for utility functions used in the pipeline.

Usage:
To use this script, ensure that all required environment variables and Airflow variables are set. The DAG can be triggered manually or scheduled to run at specified intervals.
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
file_prefix = Variable.get("SALES_FILE_PREFIX")
sales_raw_data_table_name = Variable.get("SALES_RAW_DATA_TABLE_NAME")
sales_raw_data_table_sql = Variable.get("SALES_TABLE_SQL")
sales_database = Variable.get("SALES_DATABASE")
data_directory = os.path.join(dbt_directory, 'data')

# Ensure the data directory exists; create it if necessary
os.makedirs(data_directory, exist_ok=True)

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 0  # No retries if a task fails, can be adjusted based on the use case
}


# Define the DAG for the sales data pipeline
dag = DAG(
    'sales_data_pipeline',
    default_args=default_args,
    max_active_runs=1,  # Ensures that only one instance of the DAG runs at a time
    description='Pipeline to download sales raw data from S3, ingest into Postgres, and cleanup local files.',
    schedule_interval='@daily',  # Scheduled to run daily
    start_date=days_ago(1),  # Sets the start date to one day ago
    catchup=False,  # Prevents Airflow from running missed DAG instances
    tags=["sales", env_vars["env"]]  # Tags to categorize the DAG
)


# Task to check if the table exists in PostgreSQL and create it if it doesn't
check_create_table = PythonOperator(
    task_id='check_create_table',
    python_callable=check_and_create_table,
    op_kwargs={
        'database_name': sales_database,
        'table_name': sales_raw_data_table_name,
        'postgres_conn_id': env_vars['postgres_conn_id'],
        'create_table_sql': sales_raw_data_table_sql
    },
    trigger_rule='always',  # Ensures this task always runs regardless of previous task status
    dag=dag,
)

# Task to download raw sales data CSV files from an S3 bucket to the local data directory
download_sales_data = PythonOperator(
    task_id='download_sales_data_from_s3',
    python_callable=download_files_from_s3,
    op_kwargs={
        'bucket_name': sales_bucket_name,
        'file_prefix': file_prefix,
        'local_path_dir': data_directory,
        'aws_conn_id': env_vars['aws_conn_id']
    },
    trigger_rule='all_success',  # Only runs if all upstream tasks are successful
    dag=dag,
)

# Task to truncate the target table in PostgreSQL before loading new data
truncate_target_table = PythonOperator(
    task_id='truncate_target_table',
    python_callable=truncate_table,
    op_kwargs={
        'database_name': sales_database,
        'table_name': sales_raw_data_table_name,
        'postgres_conn_id': env_vars['postgres_conn_id']
    },
    trigger_rule='all_success',  # Only runs if all upstream tasks are successful
    dag=dag,
)

# Task to ingest CSV files from the local data directory into the PostgreSQL database
ingest_sales_data = PythonOperator(
    task_id='ingest_sales_data_into_postgres',
    python_callable=ingest_files_to_postgres,
    op_kwargs={
        'local_path_dir': data_directory,
        'file_prefix': file_prefix,
        'database_name': sales_database,
        'table_name': sales_raw_data_table_name,
        'postgres_conn_id': env_vars['postgres_conn_id']
    },
    trigger_rule='all_success',  # Only runs if all upstream tasks are successful
    dag=dag,
)

# Task to clean up the local data directory after processing
cleanup_local_files = PythonOperator(
    task_id='cleanup_local_files',
    python_callable=clean_local_files,
    op_kwargs={'local_path_dir': data_directory},
    trigger_rule='all_done',  # Runs regardless of the previous task's outcome
    dag=dag,
)

# Task to notify if any task in the pipeline fails
notify_failure_email = EmailOperator(
    task_id='notify_failure_email',
    to=[email_recipient],
    subject=failure_email_subject,
    html_content=failure_email_content.replace('{dag.dag_id}', dag.dag_id),
    trigger_rule='one_failed',  # Triggers this task if any previous task fails
    dag=dag,
)

# Set task dependencies to ensure the correct order of operations
# Ensure table exists before downloading data
check_create_table >> download_sales_data  # type: ignore
# Truncate the table after downloading data
download_sales_data >> truncate_target_table  # type: ignore
# Ingest data after truncating the table
truncate_target_table >> ingest_sales_data  # type: ignore
# Clean up local files after ingestion
ingest_sales_data >> cleanup_local_files  # type: ignore

# Set failure notification for all tasks
# Notify if table creation/check fails
check_create_table >> notify_failure_email  # type: ignore
# Notify if download fails
download_sales_data >> notify_failure_email  # type: ignore
# Notify if truncation
truncate_target_table >> notify_failure_email  # type: ignore
