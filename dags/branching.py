from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta

days = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]

default_args = {
    "owner": "Fokko",
    "depends_on_past": False,
    "start_date": datetime(2018, 10, 18),
    "email": ["fokko@apache.org"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

dag = DAG("Weekday", default_args=default_args, schedule_interval="@daily")

# Returns the week day (Mon, Tue, etc.). Should match a task id.
def get_day(**context):
    return context["execution_date"].strftime("%a")


# BranchPythonOperator will use "weekday" variable, and decide which task to launch next
fork = BranchPythonOperator(
    task_id="branching", python_callable=get_day, provide_context=True, dag=dag
)

# One dummy operator for each week day, all branched to the fork
for day in days:
    fork >> DummyOperator(task_id=day, dag=dag)
