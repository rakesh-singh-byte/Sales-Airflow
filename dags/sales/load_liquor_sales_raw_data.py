"""
DAG for Liquor Sales Data Pipeline

This Airflow DAG handles the process of downloading liquor sales raw data from an S3 bucket,
ingesting it into a PostgreSQL database, and cleaning up local files. It also sends email notifications
if any task in the pipeline fails.
"""

import os

from airflow.models import Variable
from airflow.operators.email import EmailOperator
from airflow.operators.python import PythonOperator
from utils.utils import (
    check_and_create_table,
    clean_local_files,
    download_files_from_s3,
    ingest_files_to_postgres,
    truncate_table,
)

from airflow import DAG
from airflow.utils.dates import days_ago


def load_environment_variables():
    """
    Load environment variables required for the DAG.

    This function retrieves the necessary environment variables used in the Airflow DAG,
    such as AWS and Postgres connection IDs. If these environment variables are not set,
    default values are used.

    Returns:
        dict: A dictionary containing the connection IDs for AWS and Postgres, 
            with keys 'aws_conn_id' and 'postgres_conn_id'. Defaults to 'aws_default' 
            and 'postgres_default' if the corresponding environment variables are not found.

    Example:
        env_vars = load_environment_variables()
        print(env_vars['aws_conn_id'])  # Output: 'aws_default' (if AWS_CONN_ID is not set)
    """
    return {
        'aws_conn_id': os.getenv('AWS_CONN_ID', 'aws_default'),
        'postgres_conn_id': os.getenv('POSTGRES_CONN_ID', 'postgres_default')
    }


env_vars = load_environment_variables()

# Set variables
email_recipient = Variable.get(
    "EMAIL_RECIPIENT", default_var='rakesh.singh@tothenew.com')
failure_email_subject = Variable.get(
    "FAILURE_EMAIL_SUBJECT", default_var='DAG Failure Notification')
failure_email_content = Variable.get(
    "FAILURE_EMAIL_CONTENT",
    default_var="""<h3>Dear Team,</h3><p>The DAG <strong>'liquor_sales_data_pipeline'</strong> failed.</p>"""
)
dbt_directory = Variable.get(
    "DBT_DIR", default_var='/Users/rakeshsingh/Personal/dbt')
project_name = Variable.get(
    "SALES_PROJECT_NAME", default_var='Sales-Analytics')
sales_bucket_name = Variable.get(
    "SALES_BUCKET_NAME", default_var='dbt-sales-data')
file_prefix = Variable.get(
    "LIQUOR_SALES_FILE_PREFIX", default_var='Liquor_Sales_')
liquor_sales_raw_data_table_name = Variable.get(
    "LIQUOR_SALES_RAW_DATA_TABLE_NAME", default_var='raw_liquor_sales_data')
liquor_sales_raw_data_table_sql = Variable.get("LIQUOR_SALES_TABLE_SQL")
liquor_sales_database = Variable.get(
    "LIQUOR_SALES_DATABASE", default_var='sales_analytics')
data_directory = f'{dbt_directory}/{project_name}/data'

# Ensure the data directory exists
os.makedirs(data_directory, exist_ok=True)

# Define default_args
default_args = {
    'owner': 'airflow',
    'retries': 0
}

# Define the DAG
dag = DAG(
    'liquor_sales_data_pipeline',
    default_args=default_args,
    description='Pipeline to download liquor sales raw data from S3, ingest into Postgres, and cleanup local files.',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
)

# Define tasks

check_create_table = PythonOperator(
    task_id='check_create_table',
    python_callable=check_and_create_table,
    op_kwargs={
        'database_name': liquor_sales_database,
        'table_name': liquor_sales_raw_data_table_name,
        'postgres_conn_id': env_vars['postgres_conn_id'],
        'create_table_sql': liquor_sales_raw_data_table_sql
    },
    trigger_rule='always',
    dag=dag,
)

download_sales_data = PythonOperator(
    task_id='download_liquor_sales_data_from_s3',
    python_callable=download_files_from_s3,
    op_kwargs={
        'bucket_name': sales_bucket_name,
        'file_prefix': file_prefix,
        'local_path_dir': data_directory,
        'aws_conn_id': env_vars['aws_conn_id']
    },
    trigger_rule='all_success',
    dag=dag,
)

truncate_target_table = PythonOperator(
    task_id='truncate_liquor_target_table',
    python_callable=truncate_table,
    op_kwargs={
        'database_name': liquor_sales_database,
        'table_name': liquor_sales_raw_data_table_name,
        'postgres_conn_id': env_vars['postgres_conn_id']
    },
    trigger_rule='all_success',
    dag=dag,
)

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
    trigger_rule='all_success',
    dag=dag,
)

cleanup_local_files = PythonOperator(
    task_id='cleanup_liquor_local_files',
    python_callable=clean_local_files,
    op_kwargs={'local_path_dir': data_directory},
    trigger_rule='all_done',
    dag=dag,
)

notify_failure_email = EmailOperator(
    task_id='notify_failure_email',
    to=[email_recipient],
    subject=failure_email_subject,
    html_content=failure_email_content.replace('{dag.dag_id}', dag.dag_id),
    trigger_rule='one_failed',
    dag=dag,
)

# Set task dependencies
check_create_table >> download_sales_data  # type: ignore
download_sales_data >> truncate_target_table  # type: ignore
truncate_target_table >> ingest_sales_data  # type: ignore
ingest_sales_data >> cleanup_local_files  # type: ignore

# Set failure notification for all tasks
(check_create_table >> notify_failure_email)  # type: ignore
(download_sales_data >> notify_failure_email)  # type: ignore
(truncate_target_table >> notify_failure_email)  # type: ignore
(ingest_sales_data >> notify_failure_email)  # type: ignore
(cleanup_local_files >> notify_failure_email)  # type: ignore
