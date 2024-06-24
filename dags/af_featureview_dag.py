import sys
from pathlib import Path
root_path = str(Path(__file__).resolve().parents[1]) + "/"
sys.path.insert(0,root_path)

from airflow import DAG
from airflow.operators.python import PythonOperator
from utils import config
from dags import dag_utils


dag_viirs = DAG(
    'VIIRS_AF_FEATUREVIEW',
    default_args=config.default_args,
    schedule_interval='0 11 * * *',
    description='A DAG for converting VIIIRS Active Fire to FeatureView in gee',
)

convert_task_viirs = PythonOperator(
    task_id='download_task',
    python_callable=dag_utils.to_feature_view,
    op_kwargs={
        'asset_id': 'projects/ee-eo4wildfire/assets/VIIRS_AF'
    },
    dag=dag_viirs,
)

dag_modis = DAG(
    'MODIS_AF_FEATUREVIEW',
    default_args=config.default_args,
    schedule_interval='0 11 * * *',
    description='A DAG for converting MODIS Active Fire to FeatureView in gee',
)

convert_task_modis = PythonOperator(
    task_id='download_task',
    python_callable=dag_utils.to_feature_view,
    op_kwargs={
        'asset_id': 'projects/ee-eo4wildfire/assets/MODIS_AF'
    },
    dag=dag_modis,
)

convert_task_viirs
convert_task_modis
