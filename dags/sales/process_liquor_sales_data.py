"""
process_liquor_sales_data.py

This module defines an Apache Airflow DAG for processing liquor sales data using dbt (data build tool).
The DAG runs dbt commands to execute, test, and generate documentation for dbt models. It also includes error handling
and notifications for task failures.

Key Components:
- `run_dbt_command(command: str, cwd: str)`: Helper function to run dbt commands.
- `run_dbt_run()`: Task to run dbt models.
- `run_dbt_test()`: Task to run dbt tests.
- `generate_dbt_docs()`: Task to generate dbt documentation.
- `dbt_tasks`: Task group for dbt-related tasks.
- `notify_failure_email`: Task to send an email notification if any task fails.

Configuration:
- Airflow Variables:
  - `DBT_DIR`: Directory path for the DBT project
  - `SALES_PROJECT_NAME`: Name of the sales project
  - `EMAIL_RECIPIENT`: Recipient email address for failure notifications
  - `FAILURE_EMAIL_SUBJECT`: Subject line for failure notifications
  - `FAILURE_EMAIL_CONTENT`: HTML content for failure notifications

Dependencies:
- Requires `subprocess` module for running shell commands.

Usage:
To use this script, ensure that all required Airflow variables are set. The DAG can be triggered manually or scheduled to run at specified intervals.
"""

import subprocess

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.operators.email import EmailOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from utils.variables import load_environment_variables

# Load environment-specific variables
env_vars = load_environment_variables()

# Retrieve configuration settings from Airflow Variables
dbt_sales_project_directory = Variable.get(f"DBT_DIR_{env_vars['env']}")
project_name = Variable.get("SALES_PROJECT_NAME")
email_recipient = Variable.get("EMAIL_RECIPIENT")
failure_email_subject = Variable.get("FAILURE_EMAIL_SUBJECT")
failure_email_content = Variable.get("FAILURE_EMAIL_CONTENT")

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 0  # No retries if a task fails, can be adjusted based on the use case
}

# Define the DAG for processing liquor sales data using dbt
dag = DAG(
    'process_liquor_sales_data',
    default_args=default_args,
    max_active_runs=1,  # Ensures that only one instance of the DAG runs at a time
    description='Advanced DAG for running dbt models with error handling and notifications',
    schedule_interval='@daily',  # Scheduled to run daily
    start_date=days_ago(1),  # Sets the start date to one day ago
    catchup=False,  # Prevents Airflow from running missed DAG instances
    tags=['liquor_sales', env_vars["env"]]  # Tags to categorize the DAG
)


def run_dbt_command(command: str, cwd: str = dbt_sales_project_directory) -> None:
    """
    Run a dbt command in a specified directory.

    Args:
        command (str): The dbt command to execute.
        cwd (str): The working directory where the command will be executed. Default is the dbt sales project directory.

    Raises:
        AirflowException: If the command returns a non-zero exit code.
    """
    try:
        # Run the dbt command and capture the output
        result = subprocess.run(
            command,
            shell=True,  # Execute command through the shell
            check=True,  # Raise CalledProcessError if command fails
            cwd=cwd,  # Working directory for the command
            stdout=subprocess.PIPE,  # Capture standard output
            stderr=subprocess.PIPE,  # Capture standard error
        )
        # Log the standard output
        print(result.stdout.decode())
        # Log any errors if present
        if result.stderr:
            print(result.stderr.decode())
    except subprocess.CalledProcessError as e:
        # Raise an Airflow exception if the dbt command fails
        raise AirflowException(
            f"Error running dbt command '{command}': {e}") from e


def run_dbt_run() -> None:
    """
    Task function to run dbt models.
    """
    run_dbt_command("dbt run")


def run_dbt_test() -> None:
    """
    Task function to run dbt tests.
    """
    run_dbt_command("dbt test")


def generate_dbt_docs() -> None:
    """
    Task function to generate dbt documentation.
    """
    run_dbt_command("dbt docs generate")


# Define tasks in a TaskGroup for better organization of dbt-related tasks
with TaskGroup(group_id='dbt_tasks', dag=dag) as dbt_tasks:
    run_dbt = PythonOperator(
        task_id='run_dbt',
        python_callable=run_dbt_run,  # Executes dbt models
        dag=dag,
    )

    test_dbt = PythonOperator(
        task_id='test_dbt',
        python_callable=run_dbt_test,  # Runs dbt tests
        dag=dag,
    )

    generate_docs = PythonOperator(
        task_id='generate_dbt_docs',
        python_callable=generate_dbt_docs,  # Generates dbt documentation
        dag=dag,
    )

    # Set the execution order of the tasks within the TaskGroup
    run_dbt >> test_dbt >> generate_docs  # type: ignore

# Task to notify if the pipeline or any tasks fail
notify_failure_email = EmailOperator(
    task_id='notify_failure_email',
    to=[email_recipient],
    subject=failure_email_subject,
    html_content=failure_email_content.replace('{dag.dag_id}', dag.dag_id),
    trigger_rule='one_failed',  # Triggers this task if any previous task fails
    dag=dag,
)

# Set task dependencies to ensure the correct order of operations
dbt_tasks >> notify_failure_email  # type: ignore
