import os
import subprocess


os.chdir('/geoinfo_vol1/home/z/h/zhao2/LowResSatellitesService/')
import sys
sys.path.insert(0,'/geoinfo_vol1/home/z/h/zhao2/LowResSatellitesService/')

from pathlib import Path
from prettyprinter import pprint
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from utils.utils import download_af_from_firms
from datetime import timedelta
import datetime


start_date = (datetime.datetime(2023, 9, 24))
default_args = {
    'owner': 'zhaoyutim',
    'start_date': start_date,
    'depends_on_past': False,
    'email': ['zhaoyutim@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'VIIRSAF_process_and_upload',
    default_args=default_args,
    schedule_interval='0 10 * * *',
    description='A DAG for processing VIIIRS Active Fire csv and upload to gee',
)

id = 'NA'
utmzone = '4326'
roi_arg = '-138,48,-109,60'
firms = [
    "/data/active_fire/modis-c6.1/shapes/zips/MODIS_C6_1_Global_24h.zip",
    # "/data/active_fire/suomi-npp-viirs-c2/shapes/zips/SUOMI_VIIRS_C2_Global_24h.zip",
    "/data/active_fire/noaa-20-viirs-c2/shapes/zips/J1_VIIRS_C2_Global_24h.zip"
]
asset_id = f"projects/ee-eo4wildfire/assets/VIIRS_AF"
nasa_website = "https://firms.modaps.eosdis.nasa.gov"
url = nasa_website + firms[1]
url = url.replace("24h", '7d')
save_folder = Path('data/VIIRS_AF')
save_folder.mkdir(exist_ok=True)
print(save_folder)

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
        'save_folder': save_folder,
        'asset_id': asset_id
    },
    dag=dag,
)

download_task
