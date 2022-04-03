import os
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from utils import helper
from itertools import repeat
import gzip
import shutil

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
    day_data_path = os.path.join(data_path, execution_date)

    task_instance = context['task_instance']
    task_instance.xcom_push(key='execution_date', value=execution_date)
    task_instance.xcom_push(key='file_name', value=file_name)
    task_instance.xcom_push(key='day_data_path', value=day_data_path)

def download_to_local(data_path, execution_date):
    helper.parallel_process(
        func=helper.download_file,
        args_list=list(zip(repeat(execution_date), range(24), repeat(data_path))),
    )

def filter_data(data_path, execution_date, included_events=['CreateEvent']):
    in_file_paths = [os.path.join(data_path, execution_date, f"{i}.json") for i in range(24)]
    out_folder_paths = repeat(os.path.join(data_path, execution_date), 24)
    suffix = [f"{execution_date.replace('-', '')}{i:02d}" for i in range(24)]
    included_events = repeat(included_events, 24)
    mode = repeat('json', 24)

    args_list = list(zip(in_file_paths, out_folder_paths, suffix, included_events, mode))

    helper.parallel_process(
        func=helper.filter_events,
        args_list=args_list,
        process_num=4
    )

def compress_data(data_path, execution_date, included_events=['CreateEvent'], **context):

    suffix = execution_date.replace('-', '')

    task_instance = context['task_instance']
    for event in included_events:
        task_instance.xcom_push(key=event, value=f"{event}_{suffix}.json.gz")

    for event in included_events:
        with gzip.open(os.path.join(data_path, execution_date, f"{event}_{suffix}.json.gz"), 'ab', 5) as outfile:
            for i in range(24):
                with open(os.path.join(data_path, execution_date, f"{event}_{suffix}{i:02d}.json"), 'rb') as infile:
                    shutil.copyfileobj(infile,outfile)

def upload_data(data_path, execution_date, bucket, included_events=['CreateEvent']):
    
    suffix = execution_date.replace('-','')

    for event in included_events:
        helper.upload_to_gcs(
            bucket, 
            f"{event}_{suffix}.json.gz", 
            os.path.join(data_path, execution_date, f"{event}_{suffix}.json.gz")
        )

with DAG(
    dag_id="github_event_ingestion_parallel",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=True,
    max_active_runs=3,
    tags=['github']
) as dag:

    gen_params_task = PythonOperator(
        task_id="gen_params_task",
        python_callable=generate_params,
        provide_context=True
    )

    download_to_local_task = PythonOperator(
        task_id="download_to_local_task",
        python_callable=download_to_local,
        op_kwargs={
            "data_path": data_path,
            "execution_date": "{{ task_instance.xcom_pull(task_ids='gen_params_task', key='execution_date') }}",
        }
    )

    filter_data_task = PythonOperator(
        task_id="filter_data_task",
        python_callable=filter_data,
        op_kwargs={
            "data_path": data_path, 
            "execution_date": "{{ task_instance.xcom_pull(task_ids='gen_params_task', key='execution_date') }}",
        }
    )

    compress_data_task = PythonOperator(
        task_id="compress_data_task",
        python_callable=compress_data,
        op_kwargs={
            "data_path": data_path, 
            "execution_date": "{{ task_instance.xcom_pull(task_ids='gen_params_task', key='execution_date') }}",
        },
        provide_context=True
    )

    clear_raw_files_task = BashOperator(
        task_id='clear_raw_files_task',
        bash_command="""
            export EXECUTION_DATE={{ task_instance.xcom_pull(task_ids='gen_params_task', key='execution_date') }};
            cd $AIRFLOW_HOME/data/$EXECUTION_DATE;
            for i in {0..23}; do
                rm -rf $i.json
            done;
        """
    )

    upload_to_gcs_task = PythonOperator(
        task_id="upload_to_gcs_task",
        python_callable=upload_data,
        op_kwargs={
            "data_path": data_path,
            "execution_date": "{{ task_instance.xcom_pull(task_ids='gen_params_task', key='execution_date') }}",
            "bucket": bucket
        }
    )
    
    clear_local_files_task = BashOperator(
        task_id='clear_local_files_task',
        bash_command="""
            export EXECUTION_DATE={{ task_instance.xcom_pull(task_ids='gen_params_task', key='execution_date') }};
            cd $AIRFLOW_HOME/data;
            rm -rf $EXECUTION_DATE;
        """
    )

    create_CreateEvent_table_task = PythonOperator(
        task_id="create_CreateEvent_table_task", 
        python_callable=helper.gcs_to_bigquery_table,
        op_kwargs={
            "bucket": bucket,
            "file_path": "{{ task_instance.xcom_pull(task_ids='compress_data_task', key='CreateEvent') }}",
            "schema_path": schema_path,
            "destination_table_id": f"{project_id}.{dataset_id}.create_events",
            "partition_field": "created_at"
        }
    )

    # create_PushEvent_table_task = PythonOperator(
    #     task_id="create_PushEvent_table_task", 
    #     python_callable=helper.gcs_to_bigquery_table,
    #     op_kwargs={
    #         "bucket": bucket,
    #         "file_path": "{{ task_instance.xcom_pull(task_ids='compress_data_task', key='PushEvent') }}",
    #         "schema_path": schema_path,
    #         "destination_table_id": f"{project_id}.{dataset_id}.push_events",
    #         "partition_field": "created_at"
    #     }
    # )

    gen_params_task >> download_to_local_task >> filter_data_task >> compress_data_task
    compress_data_task >> upload_to_gcs_task
    compress_data_task >> clear_raw_files_task
    upload_to_gcs_task >> clear_local_files_task
    upload_to_gcs_task >> create_CreateEvent_table_task 
