import airflow
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import DAG
import random

dag = DAG(
    dag_id='example_branch_operator',
    default_args={
        'owner': 'airflow',
        'start_date': airflow.utils.dates.days_ago(2)
    },
    schedule_interval="@daily")

options = ['branch_a', 'branch_b', 'branch_c', 'branch_d']


def pick_a_random_branch():
    return random.choice(options)


branching = BranchPythonOperator(
    task_id='branching',
    python_callable=pick_a_random_branch,
    dag=dag)

for option in options:
    branching >> DummyOperator(task_id=option, dag=dag)

