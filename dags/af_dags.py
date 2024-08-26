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
from dag_utils import *
import zipfile
import glob
import geopandas
import pandas
import os.path

sources = ["VIIRS","MODIS"]
urls = [["https://firms.modaps.eosdis.nasa.gov/data/active_fire/noaa-20-viirs-c2/shapes/zips/J1_VIIRS_C2_Global_24h.zip","https://firms.modaps.eosdis.nasa.gov/data/active_fire/noaa-21-viirs-c2/shapes/zips/J2_VIIRS_C2_Global_24h.zip"],
    #"https://firms.modaps.eosdis.nasa.gov/data/active_fire/suomi-npp-viirs-c2/shapes/zips/SUOMI_VIIRS_C2_Global_24h.zip"], #VIIRS
        ["https://firms.modaps.eosdis.nasa.gov/data/active_fire/modis-c6.1/shapes/zips/MODIS_C6_1_Global_24h.zip"]] #MODIS

def download_and_upload(urls, save_folder, asset_id, gspath):
    
    for url in urls:
        download_af_from_firms(url, save_folder)
        print("Extracting...")
        
        new_file_name = os.path.split(url)[-1][:-4]
        
        zip_path = os.path.join(save_folder,new_file_name+'.zip')
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(save_folder)
        os.remove(zip_path)
        
        new_file_path, old_file_path = os.path.join(save_folder,new_file_name+".shp"), os.path.join(save_folder,"data.shp")
        file_name = "data"
        print("Reading files....")
        new_file = geopandas.read_file(new_file_path,engine='pyogrio', use_arrow=True)
        print(new_file.columns)
        new_file['ACQ_DATE'] = pandas.to_datetime(new_file['ACQ_DATE']).astype(np.int64) // 10**6
        old_file = geopandas.read_file(old_file_path,engine='pyogrio', use_arrow=True)
        print("Merging files...")
        gdf = geopandas.GeoDataFrame(pandas.concat([old_file, new_file]))

        print("Saving files...")

        gdf.to_file(os.path.join(save_folder,file_name+'.shp'),engine='pyogrio')
    
    file_name = "data"
    if os.path.isfile(os.path.join(save_folder,file_name+'.zip')):
        os.remove(os.path.join(save_folder,file_name+'.zip'))

    files_to_zip = glob.glob(os.path.join(save_folder,file_name+'.*'))

    print("Zipping...")
    zip_file_path = os.path.join(save_folder,file_name+'.zip') 
    with zipfile.ZipFile(zip_file_path, 'w') as zipf:
        for file in files_to_zip:
            zipf.write(file, os.path.basename(file))

    print("Uploading...")
    upload_to_bucket = f"gsutil -m cp -r {save_folder}/{file_name}.zip {gspath}/{file_name}.zip"
    ee_upload_table = f"earthengine upload table --force --asset_id={asset_id} {gspath}/{file_name}.zip"

    os.system(upload_to_bucket)

    ee_upload_response = subprocess.getstatusoutput(ee_upload_table)[1]
    task_id = ee_upload_response.split("ID: ")[-1]

    print(f"\n{asset_id}")
    pprint(f"task id: {task_id}")

for i in range(len(sources)):
    dag = DAG(
        f'{sources[i]}_AF_process_and_upload',
        default_args=config.default_args,
        schedule_interval='0 10 * * *',
        description=f'A DAG for processing {sources[i]} Active Fire and upload to gee',
    )
    with dag:
        download_task = PythonOperator(
            task_id='download_task',
            python_callable=download_and_upload,
            op_kwargs={
                'urls':urls[i],
                'save_folder':root_path + f'data/{sources[i]}_AF',
                'asset_id':f"projects/ee-eo4wildfire/assets/{sources[i]}_AF_2024",
                'gspath':f"gs://ai4wildfire/{sources[i].lower()}_active_fire_nrt"
            }
        )