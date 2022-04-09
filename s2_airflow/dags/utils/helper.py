from google.cloud import storage
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import os 
import json
import requests
import gzip
from billiard import Pool, Process
from . import custom_logger

logger = custom_logger.init_logger(__name__)

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
    logger.info(f"Uploading {local_file} to {bucket}")
    blob.upload_from_filename(local_file)

def gcs_to_bigquery_table(bucket, file_path, schema_path, destination_table_id, partition_field, execution_date):

    # Construct BigQuery & storage client objects.

    bigquery_client = bigquery.Client()
    storage_client = storage.Client()
    bucket_obj = storage_client.bucket(bucket)
    schema = bigquery_client.schema_from_json(schema_path)
    suffix = execution_date.replace('-', '')
    
    # Handle new write & append write
    # try: 
    #     bigquery_client.get_table(destination_table_id)
    #     job_config = bigquery.LoadJobConfig(
    #         source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    #         , schema=schema
    #         , write_disposition=bigquery.WriteDisposition.WRITE_APPEND
    #         , time_partitioning=bigquery.TimePartitioning(
    #             type_=bigquery.TimePartitioningType.DAY,
    #             field=partition_field, 
    #         )
            
    #     )
    # except NotFound:
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
        , schema=schema
        , write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
        , time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field=partition_field, 
        )
    )
    
    # Check table existence
    try: 
        bigquery_client.get_table(destination_table_id)
    except NotFound:
        logger.info(f"Table {destination_table_id} not found, creating new...")
        destination = destination_table_id
    else:
        logger.info(f"Table {destination_table_id} found, replacing partition {suffix}...")
        destination = f"{destination_table_id}${suffix}"
        
    # Check file existence
    uri = f"gs://{bucket}/{file_path}"
    logger.info(f"Checking file {uri}")
    if storage.Blob(bucket=bucket_obj, name=file_path).exists(storage_client):
        
        logger.info("File found, loading to BigQuery...")

        load_job = bigquery_client.load_table_from_uri(
            uri, destination, job_config=job_config
        )  

        load_job.result()

        destination_table = bigquery_client.get_table(destination)
        logger.info("Loaded {} rows.".format(destination_table.num_rows))
    else:
        raise Exception(f"File not found: {uri}")
        

def download_file(execution_date, hour, download_dir):
    logger.info(f"Downloading data of date {execution_date} - hour {hour}")
    url = f"https://data.gharchive.org/{execution_date}-{hour}.json.gz"
    req = requests.get(url)
    output_dir_path = os.path.join(download_dir, execution_date)
    os.makedirs(output_dir_path, exist_ok=True)

    file_path = f"{output_dir_path}/{hour}.json"
    
    logger.info(f"Decompress and write to {file_path}")
    byte_content = gzip.decompress(req.content)

    with open(file_path, 'bw') as f:
        f.write(byte_content)
    

def filter_events(
        in_file_path, 
        out_folder_path, 
        suffix,
        included_events = ['CreateEvent', 'PushEvent'],
        output = ['json', 'gz', 'return'],
    ):
    logger.info(f"Processing file {in_file_path}")
    events_store = {k: [] for k in included_events}

    with open(in_file_path, 'r') as f:
        while True:
            line = f.readline()
            if line:
                json_line = json.loads(line)
                if json_line['type'] in included_events:
                    events_store[json_line['type']].append(json_line)
                else:
                    next
            else:
                break
    if output == 'return':
        return events_store
    elif output == 'gz': 
        for key, value in events_store.items():
            with gzip.open(
                filename=os.path.join(out_folder_path, f"{key}_{suffix}.json.gz"), 
                mode='ab', 
                compresslevel=5
            ) as f:
                f.writelines(bytes(json.dumps(i) + "\n", encoding="utf-8") for i in value)
    elif output == 'json':
        for key, value in events_store.items():
            with open(os.path.join(out_folder_path, f"{key}_{suffix}.json"), 'a') as f:
                for i in value:
                    json.dump(i, f)
                    f.write("\n")
    else:
        raise ValueError(f"Output not supported: {output}")
    

def parallel_process(func, args_list, process_num=4):
    logger.info(f"Init {process_num} parallel processes")
    with Pool(process_num) as p:
        p.starmap(func, args_list)


def parallel_process_manual(func, args_list, process_num=4):
    pool = {}
    for i in range(len(args_list) // process_num):
        logger.info(f"Processing batch {i}")
        for pid, args in enumerate(args_list[i*process_num:min((i+1)*process_num,len(args_list))]):
            p = Process(target=func, args=args, daemon=False)
            pool[pid] = p
        
        for pid in pool.keys():
            pool[pid].start()
        
        for pid in pool.keys():
            pool[pid].join()
            pool[pid].close()

def sequential_process(func, args_list):
    for args in args_list:
        func(*args)

