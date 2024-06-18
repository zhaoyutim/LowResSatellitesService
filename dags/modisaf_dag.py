import os
import subprocess
import sys
from pathlib import Path
root_path = str(Path(__file__).resolve().parents[1]) + "/"
sys.path.insert(0,root_path)

import datetime
from pathlib import Path
from prettyprinter import pprint
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from datetime import timedelta
from utils.utils import download_af_from_firms

from utils import config

dag = DAG(
    'MODISAF_process_and_upload',
    default_args=config.default_args,
    schedule_interval='0 10 * * *',
    description='A DAG for processing MODIS Active Fire csv and upload to gee',
)

id = 'NA'
utmzone = '4326'
roi_arg = '-138,48,-109,60'
firms = [
    "/data/active_fire/modis-c6.1/shapes/zips/MODIS_C6_1_Global_24h.zip",
    # "/data/active_fire/suomi-npp-viirs-c2/shapes/zips/SUOMI_VIIRS_C2_Global_24h.zip",
    "/data/active_fire/noaa-20-viirs-c2/shapes/zips/J1_VIIRS_C2_Global_24h.zip"
]
asset_id = f"projects/ee-eo4wildfire/assets/MODIS_AF"
nasa_website = "https://firms.modaps.eosdis.nasa.gov"
url = nasa_website + firms[0]
url = url.replace("24h", '7d')
save_folder = Path('data/MODIS_AF')
save_folder.mkdir(exist_ok=True)

def download_and_upload(url, save_folder, asset_id, bucket="ai4wildfire"):
    filename = os.path.split(url)[-1][:-4]

    download_af_from_firms(url, save_folder)

    upload_to_bucket = f"gsutil -m cp -r {save_folder}/{filename}.zip gs://{bucket}/viirs_active_fire_nrt/{filename}.zip"
    ee_upload_table = f"earthengine upload table --force --asset_id={asset_id} gs://{bucket}/viirs_active_fire_nrt/{filename}.zip"

    os.system(upload_to_bucket)

    ee_upload_response = subprocess.getstatusoutput(ee_upload_table)[1]
    task_id = ee_upload_response.split("ID: ")[-1]

    print(f"\n{asset_id}")
    pprint(f"task id: {task_id}")

    return filename, task_id

download_task = PythonOperator(
    task_id='download_task',
    python_callable=download_and_upload,
    op_kwargs={
        'url':url,
        'save_folder':save_folder,
        'asset_id':asset_id
    },
    dag=dag,
)

download_task
