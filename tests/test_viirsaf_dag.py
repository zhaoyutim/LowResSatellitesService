import os
import subprocess
from pathlib import Path
from pprint import pprint

import xarray as xr
import rioxarray as rxr
from easydict import EasyDict as edict

# from dags.viirsaf_dag import download_and_upload
# from dags.modisaf_dag import download_and_upload
from utils.downloader import Downloader


def download_and_upload(url, save_folder, asset_id, bucket="ai4wildfire"):
    filename = os.path.split(url)[-1][:-4]

    downloader = Downloader()
    downloader.download_af_from_firms(url, save_folder)

    upload_to_bucket = f"gsutil -m cp -r {save_folder}/{filename}.zip gs://{bucket}/viirs_active_fire_nrt/{filename}.zip"
    ee_upload_table = f"earthengine upload table --force --asset_id={asset_id} gs://{bucket}/viirs_active_fire_nrt/{filename}.zip"

    os.system(upload_to_bucket)

    ee_upload_response = subprocess.getstatusoutput(ee_upload_table)[1]
    task_id = ee_upload_response.split("ID: ")[-1]

    print(f"\n{asset_id}")
    pprint(f"task id: {task_id}")

    return filename, task_id

def test_download_viirs():
    firms = [
        "/data/active_fire/modis-c6.1/shapes/zips/MODIS_C6_1_Global_24h.zip",
        # "/data/active_fire/suomi-npp-viirs-c2/shapes/zips/SUOMI_VIIRS_C2_Global_24h.zip",
        "/data/active_fire/noaa-20-viirs-c2/shapes/zips/J1_VIIRS_C2_Global_24h.zip"
    ]
    nasa_website = "https://firms.modaps.eosdis.nasa.gov"
    asset_id = f"projects/ee-eo4wildfire/assets/VIIRS_AF"
    url = nasa_website + firms[1]
    url = url.replace("24h", '7d')
    save_folder = Path('data/VIIRS_AF')
    save_folder.mkdir(exist_ok=True)
    download_and_upload(url, save_folder, asset_id, bucket="ai4wildfire")

def test_download_modis():
    firms = [
        "/data/active_fire/modis-c6.1/shapes/zips/MODIS_C6_1_Global_24h.zip",
        # "/data/active_fire/suomi-npp-viirs-c2/shapes/zips/SUOMI_VIIRS_C2_Global_24h.zip",
        "/data/active_fire/noaa-20-viirs-c2/shapes/zips/J1_VIIRS_C2_Global_24h.zip"
    ]
    nasa_website = "https://firms.modaps.eosdis.nasa.gov"
    asset_id = f"projects/ee-eo4wildfire/assets/MODIS_AF"
    url = nasa_website + firms[0]
    url = url.replace("24h", '7d')
    save_folder = Path('../data/MODIS_AF')
    save_folder.mkdir(exist_ok=True)
    download_and_upload(url, save_folder, asset_id, bucket="ai4wildfire")

if __name__=='__main__':
    test_download_modis()