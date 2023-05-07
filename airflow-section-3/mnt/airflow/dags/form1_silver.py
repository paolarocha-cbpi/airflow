from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.transfers.gcs_to_local import GCSToLocalFilesystemOperator
import logging
import pandas as pd

## tasks
## task1: sense a new file in gcp and get the new filename
## task2: read the new file

BUCKET_NAME='silver_test'

@task
def get_last_file_added(prefix):
    gsc_hook = GCSHook(gcp_conn_id='gcs_silver_test')
    files = gsc_hook.list_by_timespan(
        bucket_name=BUCKET_NAME,
        timespan_start=datetime.utcnow() - timedelta(minutes=10),
        timespan_end=datetime.utcnow(),
        versions=None,
        max_results=None,
        prefix=prefix,
        delimiter='/'
    )
    logging.info(f'Files of bucket: {files}')
    # TODO: get the last file
    return files

@task
def fillout_to_df(fillout_path):
    with open(fillout_path) as fillout_file:
        file_contents = fillout_file.load()

    id_user = file_contents["id_user"]
    df = pd.DataFrame()
    for answer in file_contents["answers"]:
        answer['id_user'] = id_user
        df = df.append(answer, ignore_index=True)

    return df


with DAG(
    "test-form1",
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        "depends_on_past": False,
        "email": ["airflow@example.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        # 'queue': 'bash_queue',
        # 'pool': 'backfill',
        # 'priority_weight': 10,
        # 'end_date': datetime(2016, 1, 1),
        # 'wait_for_downstream': False,
        # 'sla': timedelta(hours=2),
        # 'execution_timeout': timedelta(seconds=300),
        # 'on_failure_callback': some_function,
        # 'on_success_callback': some_other_function,
        # 'on_retry_callback': another_function,
        # 'sla_miss_callback': yet_another_function,
        # 'trigger_rule': 'all_success'
    },
    description="Test to read a new file in GCP",
    schedule=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["example"],
) as dag:
    
    file = get_last_file_added('id_organization_test/id_fill_out/files/')

    # download_file = GCSToLocalFilesystemOperator(
    #     task_id="download_fill_out",
    #     object_name=file,
    #     bucket=BUCKET_NAME,
    #     filename=f'/tmp/{file}',
    # )

    # fillout_to_df(f'/tmp/{file}')

