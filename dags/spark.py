import airflow
from airflow import DAG
from operators.spark_submit_operator import SparkSubmitOperator

dag = DAG(
    dag_id="hello_spark",
    default_args={
        "owner": "godatadriven",
        "start_date": airflow.utils.dates.days_ago(22),
    },
)

SparkSubmitOperator(
    task_id="compute_pi",
    application='local:///dags/spark/pi.py',
    conn_id='local-spark',
    conf={
        'spark.kubernetes.driver.volumes.persistentVolumeClaim.shared.mount.path': '/dags',
        'spark.kubernetes.driver.volumes.persistentVolumeClaim.shared.mount.readOnly': 'true',
        'spark.kubernetes.driver.volumes.persistentVolumeClaim.shared.options.claimName': 'airflow-dags',

        'spark.kubernetes.executor.volumes.persistentVolumeClaim.shared.mount.path': '/dags',
        'spark.kubernetes.executor.volumes.persistentVolumeClaim.shared.mount.readOnly': 'true',
        'spark.kubernetes.executor.volumes.persistentVolumeClaim.shared.options.claimName': 'airflow-dags',

        'spark.kubernetes.container.image': 'fokkodriesprong/spark-py:2.4.3'
    },
    dag=dag
)
