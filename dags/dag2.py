import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

args = {"owner": "godatadriven", "start_date": airflow.utils.dates.days_ago(14)}

dag = DAG(
    dag_id="exercise2",
    default_args=args,
    description="Sample DAG showing some Airflow Operators."
)


def _print_exec_date(execution_date, **context):
    print(execution_date)


print_date = PythonOperator(
    task_id="print_execution_date",
    python_callable=_print_exec_date,
    provide_context=True,
    dag=dag,
)

the_end = DummyOperator(task_id="the_end", dag=dag)

wait_tasks = [
    BashOperator(task_id="wait_" + str(w), bash_command="sleep " + str(w), dag=dag)
    for w in [1, 5, 10]
]

print_date >> wait_tasks >> the_end
