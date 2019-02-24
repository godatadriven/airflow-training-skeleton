import airflow
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator, PythonOperator
from airflow.utils.trigger_rule import TriggerRule

args = {"owner": "godatadriven", "start_date": airflow.utils.dates.days_ago(14)}

dag = DAG(
    dag_id="exercise3",
    default_args=args,
    description="Demo DAG showing BranchPythonOperator.",
    schedule_interval="0 0 * * *",
)


def print_weekday(execution_date, **context):
    print(execution_date.strftime("%a"))


print_weekday = PythonOperator(
    task_id="print_weekday",
    python_callable=print_weekday,
    provide_context=True,
    dag=dag,
)

# Definition of who is emailed on which weekday
weekday_person_to_email = {
    0: "Bob",  # Monday
    1: "Joe",  # Tuesday
    2: "Alice",  # Wednesday
    3: "Joe",  # Thursday
    4: "Alice",  # Friday
    5: "Alice",  # Saturday
    6: "Alice",  # Sunday
}


# Function returning name of task to execute
def _get_person_to_email(execution_date, **context):
    person = weekday_person_to_email[execution_date.weekday()]
    return "email_" + str(person.lower())


# Branching task, the function above is passed to python_callable
branching = BranchPythonOperator(
    task_id="branching",
    python_callable=_get_person_to_email,
    provide_context=True,
    dag=dag
)

# Execute branching task after create_report task
print_weekday >> branching

final_task = BashOperator(
    task_id="final_task",
    trigger_rule=TriggerRule.ONE_SUCCESS,
    bash_command="sleep 5",
    dag=dag
)

# Create dummy tasks for names in the dict, and execute all after the branching task
for name in set(weekday_person_to_email.values()):
    email_task = DummyOperator(task_id="email_" + str(name.lower()), dag=dag)
    branching >> email_task >> final_task
