import os

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from google.cloud import storage
from google.cloud import bigquery
from utils.helper import upload_to_gcs, gcs_to_bigquery_table
import datetime


project_id = os.environ.get('PROJECT_ID')
dataset_id = os.environ.get('DATASET_ID')
table_id = os.environ.get('TABLE_ID')
bucket = os.environ.get('BUCKET')
local_home_path = os.environ.get("AIRFLOW_HOME", "/opt/airflow")
data_path = "/opt/airflow/data"
schema_path = "./dags/schemas/events.json"


default_args = {
    "owner": "airflow",
    "start_date": "2019-01-01",
    "end_date": "2019-12-31",
    "depend_on_past": False,
    "retries": 1,
}

def generate_params(**context):
    execution_date = context['ds']
    file_name = f"{execution_date}.json.gz"
    
    task_instance = context['task_instance']
    task_instance.xcom_push(key='execution_date', value=execution_date)
    task_instance.xcom_push(key='file_name', value=file_name)

with DAG(
    dag_id="github_event_ingestion_2021",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=True,
    max_active_runs=5,
    tags=['github']
) as dag:

    generate_params_task = PythonOperator(
        task_id="generate_params_task",
        python_callable=generate_params,
        provide_context=True
    )

    download_events_task = BashOperator(
        task_id="download_events_task",
        bash_command="""
            export EXECUTION_DATE={{ task_instance.xcom_pull(task_ids='generate_params_task', key='execution_date') }}
            export FILE_NAME={{ task_instance.xcom_pull(task_ids='generate_params_task', key='file_name') }}
            cd $AIRFLOW_HOME/data;
            mkdir $EXECUTION_DATE;
            for i in {0..23}; do
                curl -sSLf https://data.gharchive.org/$EXECUTION_DATE-$i.json.gz > $EXECUTION_DATE/$i.json.gz
            done;
            cat $EXECUTION_DATE/* > $FILE_NAME;
        """
    )
    
    upload_to_gcs_task = PythonOperator(
        task_id="upload_to_gcs_task",
        python_callable = upload_to_gcs,
        op_kwargs={
            "bucket": bucket,
            "object_name": "{{ task_instance.xcom_pull(task_ids='generate_params_task', key='file_name') }}",
            "local_file": data_path + "/{{ task_instance.xcom_pull(task_ids='generate_params_task', key='file_name') }}",
        }
    )
    
    clear_local_files_task = BashOperator(
        task_id='clear_local_files_task',
        bash_command="""
            export EXECUTION_DATE={{ task_instance.xcom_pull(task_ids='generate_params_task', key='execution_date') }};
            export FILE_NAME={{ task_instance.xcom_pull(task_ids='generate_params_task', key='file_name') }};
            cd $AIRFLOW_HOME/data;
            rm -rf $EXECUTION_DATE $FILE_NAME;
        """
    )
    
    gcs_to_bigquery_table_task = PythonOperator(
        task_id="gcs_to_bigquery_table_task", 
        python_callable=gcs_to_bigquery_table,
        op_kwargs={
            "bucket": bucket,
            "file_path": "{{ task_instance.xcom_pull(task_ids='generate_params_task', key='file_name') }}",
            "schema_path": schema_path,
            "destination_table_id": f"{project_id}.{dataset_id}.{table_id}",
            "partition_field": "created_at"
        }
    )
    
    generate_params_task >> download_events_task >> upload_to_gcs_task >> clear_local_files_task >> gcs_to_bigquery_table_task