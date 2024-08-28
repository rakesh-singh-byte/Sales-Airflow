"""
DAG to process all sales data using dbt Cloud.

This DAG triggers a dbt Cloud job and sends an email notification if the job fails. 
Environment-specific variables are loaded dynamically, and the DAG is scheduled to run daily.
"""

from airflow import DAG
from airflow.models import Variable
from airflow.operators.email import EmailOperator
from airflow.utils.dates import days_ago
from astronomer.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
from utils.variables import load_environment_variables

# Load necessary variables from Airflow Variables
email_recipient = Variable.get("EMAIL_RECIPIENT")
failure_email_subject = Variable.get("FAILURE_EMAIL_SUBJECT")
failure_email_content = Variable.get("FAILURE_EMAIL_CONTENT")
job_id = Variable.get("JOB_ID")

# Load environment-specific variables
env_vars = load_environment_variables()

# Define the DAG
dag = DAG(
    dag_id='process_all_sales_data_dbt_cloud',
    default_args=env_vars["default_args"],
    schedule_interval='@daily',  # Adjust this to your desired schedule interval
    start_date=days_ago(1),
    catchup=False,  # Do not backfill runs
    tags=['dbt-cloud', env_vars["env"]]
)

# Task: Trigger a dbt Cloud job
run_dbt_cloud_job = DbtCloudRunJobOperator(
    task_id='run_dbt_cloud_job',
    job_id=job_id,  # dbt Cloud job ID
    check_interval=60,  # Check every 60 seconds for job completion
    timeout=3600,  # Timeout after 1 hour
    # Use the environment-specific connection
    dbt_cloud_conn_id=env_vars["dbt_cloud_conn_id"],
    dag=dag
)

# Task: Send an email notification if any task fails
notify_failure_email = EmailOperator(
    task_id='notify_failure_email',
    to=[email_recipient],
    subject=failure_email_subject,
    html_content=failure_email_content.replace('{dag.dag_id}', dag.dag_id),
    trigger_rule='one_failed',  # Run this task only if any previous task fails
    dag=dag
)

# Set task dependencies
run_dbt_cloud_job >> notify_failure_email  # type: ignore
