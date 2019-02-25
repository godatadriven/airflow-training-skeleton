import datetime
import airflow

from airflow import DAG
from airflow.utils.trigger_rule import TriggerRule

from airflow.contrib.operators.dataflow_operator import DataFlowPythonOperator
from airflow.contrib.operators.dataproc_operator import (
    DataprocClusterCreateOperator,
    DataprocClusterDeleteOperator,
    DataProcPySparkOperator,
)

from godatadriven.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from godatadriven.operators.postgres_to_gcs import PostgresToGoogleCloudStorageOperator

from http_to_gcs_operator import HttpToGcsOperator

PROJECT_ID = "gdd-airflow-training"
BUCKET = "airflow-training-data"

args = {
    "owner": "airflow",
    "start_date": airflow.utils.dates.days_ago(14)
}

dag = DAG(
    dag_id="real-estate",
    schedule_interval="@daily",
    catchup=True,
    default_args=args,
)

dataproc_create_cluster = DataprocClusterCreateOperator(
    task_id="create_dataproc",
    cluster_name="analyse-pricing-{{ ds }}",
    project_id=PROJECT_ID,
    num_workers=2,
    zone="europe-west4-a",
    dag=dag,
)

pgsl_to_gcs = (
        PostgresToGoogleCloudStorageOperator(
            task_id="postgres_to_gcs",
            dag=dag,
            sql="SELECT * FROM land_registry_price_paid_uk WHERE transfer_date = '{{ ds }}'",
            bucket="airflow-training-data",
            filename="land_registry_price_paid_uk/{{ ds }}/properties_{}.json",
            postgres_conn_id="airflow-training-postgres",
        ) >> dataproc_create_cluster
)

for currency in {"EUR", "USD"}:
    HttpToGcsOperator(
        task_id="get_currency_" + currency,
        method="GET",
        endpoint="/airflow-training-transform-valutas?date={{ ds }}&from=GBP&to="
                 + currency,
        http_conn_id="airflow-training-currency-http",
        gcs_path="currency/{{ ds }}-" + currency + ".json",
        gcs_bucket="airflow-training-data",
        dag=dag,
    ) >> dataproc_create_cluster

compute_aggregates = DataProcPySparkOperator(
    task_id="compute_aggregates",
    main="gs://airflow-training-data/build_statistics.py",
    cluster_name="analyse-pricing-{{ ds }}",
    arguments=["{{ ds }}"],
    dag=dag,
)

dataproc_delete_cluster = DataprocClusterDeleteOperator(
    task_id="delete_dataproc",
    cluster_name="analyse-pricing-{{ ds }}",
    dag=dag,
    project_id=PROJECT_ID,
    trigger_rule=TriggerRule.ALL_DONE,
)

dataproc_create_cluster >> compute_aggregates >> dataproc_delete_cluster

compute_aggregates >> GoogleCloudStorageToBigQueryOperator(
    task_id="write_to_bq",
    bucket=BUCKET,
    source_objects=["average_prices/transfer_date={{ ds }}/*.parquet"],
    destination_project_dataset_table="gdd-airflow-training:prices.land_registry_price${{ ds_nodash }}",
    source_format="PARQUET",
    write_disposition="WRITE_TRUNCATE",
    dag=dag,
)

load_into_bigquery = DataFlowPythonOperator(
    task_id="land_registry_prices_to_bigquery",
    dataflow_default_options={
        "project": "gdd-airflow-training",
        "region": "europe-west1",
    },
    py_file="gs://europe-west1-airflow-traini-627000e4-bucket/other/dataflow_job.py",
    dag=dag,
)

pgsl_to_gcs >> load_into_bigquery
