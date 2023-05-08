from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'Processing VIIRS Images',
    default_args=default_args,
    description='A DAG for downloading and processing satellite images',
    schedule_interval=timedelta(days=1),
)

t1 = BashOperator(
    task_id='download_images',
    bash_command='python /path/to/download.py --pid {{ dag_run.conf["pid"] }} --mode {{ dag_run.conf["mode"] }} --y {{ dag_run.conf["y"] }} --roi {{ dag_run.conf["roi"] }} --sd {{ dag_run.conf["sd"] }} --ed {{ dag_run.conf["ed"] }} --rs {{ dag_run.conf["rs"] }}',
    dag=dag,
)

t2 = BashOperator(
    task_id='process_images',
    bash_command='python /path/to/process.py --pid {{ dag_run.conf["pid"] }} --mode {{ dag_run.conf["mode"] }} --y {{ dag_run.conf["y"] }} --roi {{ dag_run.conf["roi"] }} --sd {{ dag_run.conf["sd"] }} --ed {{ dag_run.conf["ed"] }} --rs {{ dag_run.conf["rs"] }}',
    dag=dag,
)

t1 >> t2
