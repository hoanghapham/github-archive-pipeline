from google.cloud import storage
from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud.exceptions import NotFound
import os 
import json
import requests
import gzip
from multiprocessing import Pool
from custom_logger import init_logger

logger = init_logger(__name__)

def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)

def gcs_to_bigquery_table(bucket, file_path, schema_path, destination_table_id, partition_field):

    # Construct BigQuery & storage client objects.

    bigquery_client = bigquery.Client()
    storage_client = storage.Client()
    bucket_obj = storage_client.bucket(bucket)
    schema = bigquery_client.schema_from_json(schema_path)

    # Handle new write & append write
    try: 
        bigquery_client.get_table(destination_table_id)
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
            , schema=schema
            , write_disposition=bigquery.WriteDisposition.WRITE_APPEND
            , time_partitioning=bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field=partition_field, 
            )
            
        )
    except NotFound:
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
            , schema=schema
            , write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
            , time_partitioning=bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field=partition_field, 
            )
        )
        
    # Check file existence
    uri = f"gs://{bucket}/{file_path}"
    logger.info(f"Checking file {uri}")
    if storage.Blob(bucket=bucket_obj, name=file_path).exists(storage_client):
        
        logger.info("File found, loading to BigQuery...")

        load_job = bigquery_client.load_table_from_uri(
            uri, destination_table_id, job_config=job_config
        )  

        load_job.result()

        destination_table = bigquery_client.get_table(destination_table_id)
        logger.info("Loaded {} rows.".format(destination_table.num_rows))
    else:
        raise Exception(f"File not found: {uri}")
        

def download_files(execution_date, hour, download_dir):
    logger.info(f"Downloading data of date {execution_date} - hour {hour}")
    url = f"https://data.gharchive.org/{execution_date}-{hour}.json.gz"
    req = requests.get(url)
    output_dir_path = f"{download_dir}/{execution_date}"
    os.makedirs(output_dir_path, exist_ok=True)

    byte_content = gzip.decompress(req.content)

    with open(f"{output_dir_path}/{hour}.json", 'bw') as f:
        f.write(byte_content)
    

def filter_events(in_file_path, out_dir_path, out_suffix):
    logger.info("Filtering events")
    create_events = []
    push_events = []

    with open(in_file_path, 'r') as f:
        while True:
            line = f.readline()
            if line:
                json_line = json.loads(line)
                if json_line['type'] == 'CreateEvent':
                    create_events.append(json_line)
                elif json_line['type'] == 'PushEvent':
                    push_events.append(json_line)
                else:
                    next
            else:
                break
    with open(f"{out_dir_path}/create_events_{out_suffix}.json", 'a') as f:
        json.dump(create_events, f)
    with open(f"{out_dir_path}/push_events_{out_suffix}.json", 'a') as f:
        json.dump(push_events, f)

def parallel_process(func, args_list, process_num=4):
    logger.info(f"Init {process_num} parallel processes")
    with Pool(process_num) as p:
        p.starmap(func, args_list)
