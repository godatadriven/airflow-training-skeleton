import airflow
from airflow import DAG
from airflow.contrib.operators.dataflow_operator import DataFlowPythonOperator
from airflow.contrib.operators.dataproc_operator import (
    DataprocClusterCreateOperator,
    DataprocClusterDeleteOperator,
    DataProcPySparkOperator,
)
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.utils.trigger_rule import TriggerRule

from airflow_training.operators.postgres_to_gcs import PostgresToGoogleCloudStorageOperator
from http_to_gcs_operator import HttpToGcsOperator

import os

PROJECT_ID = "airflowbolcom-020ce46afe7b0fe0"
BUCKET = "fokkos-bucket"

dir_path = os.path.dirname(os.path.realpath(__file__))

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

pgsl_to_gcs = PostgresToGoogleCloudStorageOperator(
    task_id="postgres_to_gcs",
    dag=dag,
    sql="SELECT * FROM land_registry_price_paid_uk WHERE transfer_date = '{{ ds }}'",
    bucket=BUCKET,
    filename="land_registry_price_paid_uk/{{ ds }}/properties_{}.json",
    postgres_conn_id="airflow-training-postgres",
) >> dataproc_create_cluster

for currency in {"EUR", "USD"}:
    HttpToGcsOperator(
        task_id="get_currency_" + currency,
        method="GET",
        endpoint="/airflow-training-transform-valutas?date={{ ds }}&from=GBP&to="
                 + currency,
        http_conn_id="airflow-training-currency-http",
        gcs_path="currency/{{ ds }}-" + currency + ".json",
        gcs_bucket=BUCKET,
        dag=dag,
    ) >> dataproc_create_cluster

compute_aggregates = DataProcPySparkOperator(
    task_id="compute_aggregates",
    main="gs://europe-west1-training-airfl-52127ea6-bucket/other/build_statistics.py",
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
        "project": PROJECT_ID,
        "region": "europe-west1",
        "staging_location": "gs://{}/dataflow-staging".format(BUCKET),
        "temp_location": "gs://{}/dataflow-staging".format(BUCKET),
        "input": "",
        "runner": "DataflowRunner",
        "job_name": "import-raw-data-{{ ds }}"
    },
    py_file="gs://europe-west1-training-airfl-52127ea6-bucket/other/dataflow_job.py",
    dag=dag,
)

pgsl_to_gcs >> load_into_bigquery
