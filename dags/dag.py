import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

dag = DAG(
    dag_id="my_first_dag",
    schedule_interval="30 7 * * *",
    default_args={
        "owner": "airflow",
        "start_date": airflow.utils.dates.days_ago(3),
        "depends_on_past": True,
        "email_on_failure": True,
        "email": "airflow_errors@myorganisation.com",
    },
)


def print_exec_date(**context):
    print(context["execution_date"])


print_exec_date = PythonOperator(
    task_id="print_exec_date",
    python_callable=print_exec_date,
    provide_context=True,
    dag=dag,
)
