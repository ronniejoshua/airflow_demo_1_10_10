from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.models import Variable

# Import our custom operator and sensor
from airflow.operators.bigquery_plugin import (
    BigQueryDataValidationOperator,
    BigQueryDatasetSensor,
)

default_arguments = {"owner": "Ronnie Joshua", "start_date": days_ago(1)}
PROJECT_ID = Variable.get("project")


with DAG(
    "bigquery_data_validation",
    schedule_interval="@daily",
    catchup=False,
    default_args=default_arguments,
    user_defined_macros={"project": PROJECT_ID},
) as dag:

    is_table_empty = BigQueryDataValidationOperator(
        task_id="is_table_empty",
        sql="SELECT COUNT(*) FROM `{{ project }}.vehicle_analytics.historical`",
        location="europe-west2",
    )

    dataset_exists = BigQueryDatasetSensor(
        task_id="dataset_exists",
        project_id="{{ project }}",
        dataset_id="vehicle_analytics",
    )

# Task Dependency 
dataset_exists >> is_table_empty