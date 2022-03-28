import pandas as pd
import pyarrow.csv as pv
import pyarrow.parquet as pq
import pyarrow
from google.cloud import storage
from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud.exceptions import NotFound
import os 

def format_to_parquet(src_file):
    if not src_file.endswith('.csv'):
        logging.error("Can only accept source files in CSV format, for the moment")
        return
    table = pyarrow.csv.read_csv(src_file)
    pyarrow.parquet.write_table(table, src_file.replace('.csv', '.parquet'))

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
    print(f"Checking file {uri}")
    if storage.Blob(bucket=bucket_obj, name=file_path).exists(storage_client):
        
        print("File found, loading to BigQuery...")

        load_job = bigquery_client.load_table_from_uri(
            uri, destination_table_id, job_config=job_config
        )  

        load_job.result()

        destination_table = bigquery_client.get_table(destination_table_id)
        print("Loaded {} rows.".format(destination_table.num_rows))
    else:
        raise Exception(f"File not found: {uri}")
        