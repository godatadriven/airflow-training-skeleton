import airflow

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

dag = DAG(
    dag_id='bigquery-to-slack',
    schedule_interval='@daily',
    default_args={
        'owner': 'GoDataDriven',
        'start_date': airflow.utils.dates.days_ago(12)
    }
)

from bigquery_get_data import BigQueryGetDataOperator

bq_fetch_data = BigQueryGetDataOperator(
    task_id='bq_fetch_data',
    sql="""
    SELECT committer.name, count(1) as cnt
    FROM [bigquery-public-data.github_repos.commits]
    WHERE DATE(committer.date) = '{{ ds }}'
    AND repo_name LIKE '%airflow%'
    GROUP BY committer.name
    ORDER BY cnt DESC
    LIMIT 5;
""",
    dag=dag
)

send_to_slack = PythonOperator(
    task_id='send_to_slack',
    python_callable=send_to_slack_func,
    provide_context=True,
    dag=dag,
)

'xoxp-559854890739-559228586160-560368279685-30c1e30ee86fff97ccfcaee36719d845'

bq_fetch_data >> send_to_slack
