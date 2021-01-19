"""
## Example PySpark dag
This example dag walks you through the concepts of branching, subdags and trigger rules.
It creates a Dataproc cluster in Google Cloud and runs a series of PySpark jobs.
"""


from airflow import DAG
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.contrib.operators.dataproc_operator import (
    DataprocClusterCreateOperator,
    DataProcPySparkOperator,
    DataprocClusterDeleteOperator,
)

from airflow.utils.dates import days_ago
from datetime import datetime
from airflow.models import Variable
from pyspark_subdag import weekday_subdag


PROJECT_ID = Variable.get("project")
default_arguments = {"owner": "Ronnie Joshua", "start_date": days_ago(1)}


def assess_day(execution_date=None):
    date = datetime.strptime(execution_date, "%Y-%m-%d")
    if date.isoweekday() < 6:
        return "weekday_analytics"
    return "weekend_analytics"


with DAG(
    "bigquery_data_analytics",
    schedule_interval="0 20 * * *",
    catchup=False,
    default_args=default_arguments,
    user_defined_macros={"project": PROJECT_ID}
) as dag:

    # https://airflow.apache.org/docs/apache-airflow/stable/tutorial.html#adding-dag-and-tasks-documentation
    dag.doc_md = __doc__

    create_cluster = DataprocClusterCreateOperator(
        task_id="create_cluster",
        project_id="{{ project }}",
        cluster_name="spark-cluster-{{ ds_nodash }}",
        num_workers=2,
        storage_bucket="rj-aa-logistics-spark-bucket",
        zone="us-east4-a",
        master_machine_type='n1-standard-1',
        worker_machine_type='n1-standard-1',
    )

    create_cluster.doc_md = """## Create Dataproc cluster
    This task creates a Dataproc cluster in your project.
    """

    # https://airflow.apache.org/docs/apache-airflow/stable/concepts.html?highlight=branch#branching
    weekday_or_weekend = BranchPythonOperator(
        task_id="weekday_or_weekend",
        python_callable=assess_day,
        op_kwargs={"execution_date": "{{ ds }}"}
    )

    weekend_analytics = DataProcPySparkOperator(
        task_id="weekend_analytics",
        main="gs://rj-aa-logistics-spark-bucket/pyspark/weekend/gas_composition_count.py",
        cluster_name="spark-cluster-{{ ds_nodash }}",
        dataproc_pyspark_jars="gs://spark-lib/bigquery/spark-bigquery-latest.jar"
    )

    # http://airflow.apache.org/docs/apache-airflow/stable/_api/airflow/operators/subdag/index.html
    weekday_analytics = SubDagOperator(
        task_id="weekday_analytics",
        subdag=weekday_subdag(
            parent_dag="bigquery_data_analytics",
            task_id="weekday_analytics",
            schedule_interval="0 20 * * *",
            default_args=default_arguments
        )
    )

    # https://airflow.apache.org/docs/apache-airflow/1.10.6/_api/airflow/contrib/operators/dataproc_operator/index.html
    delete_cluster = DataprocClusterDeleteOperator(
        task_id="delete_cluster",
        project_id="{{ project }}",
        cluster_name="spark-cluster-{{ ds_nodash }}",

        # https://airflow.apache.org/docs/apache-airflow/stable/concepts.html#trigger-rules
        trigger_rule="all_done"
    )

create_cluster >> weekday_or_weekend >> [
    weekend_analytics,
    weekday_analytics
] >> delete_cluster
