"""
variables.py

This module defines a function to load environment-specific variables required for Apache Airflow DAGs.
It retrieves AWS and Postgres connection IDs based on the current environment, allowing seamless switching
between different environments (e.g., development, production).

Key Components:
- `load_environment_variables()`: Retrieves and formats environment-specific connection IDs for AWS and Postgres.

Dependencies:
- Requires the `airflow.models.Variable` module for accessing Airflow Variables.

Usage:
This module is intended to be imported into an Airflow DAG script. The `load_environment_variables` function
can be used to fetch the appropriate connection IDs for the current environment.
"""

from airflow.models import Variable

# Retrieve the current environment from Airflow Variables
env = Variable.get("ENVIRONMENT")


def load_environment_variables():
    """
    Load environment-specific variables required for the Airflow DAG.

    This function retrieves the necessary environment variables used in the Airflow DAG,
    such as AWS and Postgres connection IDs. The connection IDs are dynamically
    constructed based on the current environment (e.g., 'dev', 'prod'), allowing
    for easy switching between environments. Default values are used if the corresponding
    environment variables are not set.

    Returns:
        dict: A dictionary containing:
            - 'aws_conn_id': AWS connection ID formatted based on the current environment.
            - 'postgres_conn_id': Postgres connection ID formatted based on the current environment.
            - 'env': The current environment (e.g., 'dev', 'prod').

    Example:
        env_vars = load_environment_variables()
        print(env_vars['aws_conn_id'])  # Output: 'aws_default_dev' (if ENVIRONMENT is set to 'dev')
    """
    # Construct and return a dictionary with connection IDs and environment
    return {
        'aws_conn_id': f'aws_default_{env}',
        'postgres_conn_id': f'postgres_default_{env}',
        'dbt_cloud_conn_id': f'dbt_cloud_default_{env}',
        'env': env,
        'default_args': {
            'owner': 'airflow',
            'retries': 0  # No retries if a task fails, can be adjusted based on the use case
        }
    }
