import os

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from google.cloud import storage
from google.cloud import bigquery

project_id = "personal-39217"
bucket = "personal-39217_raw_github_archive"
dataset_id = "github"
data_path = "/data"
base_dataset_url = "https://data.gharchive.org/2015-01-01-{0..23}.json.gz"

default_args = {
    "owner": "airflow",
    "start_date": "2022-03-20",
    "depend_on_past": False,
    "retries": 1,
}

def generate_params(**context):
    execution_date = context['ds']

    task_instance = context['task_instance']
    task_instance.xcom_push(key='execution_date', value=execution_date)

def download_data_daily(date):
    for i in range(24):


with DAG(
    dag_id="github_event_ingestion",
    schedule_interval="@hourly",
    default_args=default_args,
    catchup=True,
    max_active_runs=3,
    tags=['github']
) as dag:

    task_generate_params = PythonOperator(
        task_id="task_generate_params",
        python_callable=generate_params,
        provide_context=True
    )

    task_download_events = BashOperator(
        task="task_download_events",
        bash_command="""
            rm -rf tmp/ {{ task_instance.xcom_pull(task_ids='generate_params_task', key='dataset_url') }}.json.gz;
            for i in {0..23}; do
            wget -P tmp https://data.gharchive.org/2015-01-01-$i.json.gz -nv -a 2015-01-01.json.gz
            done;
            cat tmp/* > 2015-01-01.json.gz;
        """
    )