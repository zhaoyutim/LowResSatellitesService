import glob
import os

os.chdir('/geoinfo_vol1/home/z/h/zhao2/LowResSatellitesService/')
import sys
sys.path.insert(0,'/geoinfo_vol1/home/z/h/zhao2/LowResSatellitesService/')

import multiprocessing
from easydict import EasyDict as edict
from pathlib import Path
import rioxarray as rxr
from prettyprinter import pprint
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from utils.args import default_args
import datetime

from utils.utils import upload_hdf

''' Configuration '''
CFG = {
    'VNP09GA': {
        "products_id": "VNP09GA",
        "collection_id": '5000',
        "eeImgColName": "VIIRS_NRT",
        "format": '.h5',
        "BANDS": ["M3", "M4", "M5", "M7", "M10", "M11", "QF2"]
    },

    'VNP09_NRT': {
        "products_id": "VNP09_NRT",
        "collection_id": '5000',
        "eeImgColName": "VIIRS_NRT",
        "format": '.hdf',
        "BANDS": [
            "375m Surface Reflectance Band I1",
            "375m Surface Reflectance Band I2",
            "375m Surface Reflectance Band I1"
        ]
    },

    'MOD09GA': {
        "products_id": "MOD09GA",
        "collection_id": '61',
        "eeImgColName": "MODIS_NRT",
        "format": '.hdf',
        "BANDS": [
            "sur_refl_b01_1",
            "sur_refl_b02_1",
            # "sur_refl_b03_1",
            # "sur_refl_b04_1",
            "sur_refl_b07_1"
        ]
    },

    'MOD02HKM': {
        "products_id": "MOD02HKM",
        "collection_id": '61',
        "eeImgColName": "MODIS_NRT",
        "format": '.hdf',
        "BANDS": ["sur_refl_b01_1", "sur_refl_b02_1", "sur_refl_b03_1", "sur_refl_b04_1", "sur_refl_b07_1"]
    },
}

dag = DAG(
    'MODIS_process_and_upload_NA',
    default_args=default_args,
    description='A DAG for processing North America MODIS images and upload to gee',
)

dir_data = Path('/geoinfo_vol1/home/z/h/zhao2/LowResSatellitesService/data/MOD09GA')
dir_tif = Path('/geoinfo_vol1/home/z/h/zhao2/LowResSatellitesService/data/MOD09GATIF')
dn = ['D']
id = 'NA'
utmzone = '4326'
roi_arg = '-138,48,-109,60'
# North America
hh_list = ['08', '09', '10', '11', '12', '13', '14']
vv_list = ['02', '03', '04', '05']
SOURCE = edict(CFG['MOD09GA'])
products_id = SOURCE.products_id
collection_id = SOURCE.collection_id
asset_id = 'projects/ee-eo4wildfire/assets/MODIS_NA/'
print(dir_data)
def download_files(id, start_date, dir_data, collection_id, products_id, hh_list=['10', '11'], vv_list =['03']):
    year = start_date[:4]
    julian_day = datetime.datetime.strptime(start_date, '%Y-%m-%d').timetuple().tm_yday
    dir_data_with_id = dir_data.joinpath(id)
    dir_data_with_id.mkdir(exist_ok=True)
    url_part = f"{collection_id}/{products_id}/{year}/{julian_day}/"
    dir_data_with_id.joinpath(start_date).mkdir(exist_ok=True)
    token = 'eyJ0eXAiOiJKV1QiLCJvcmlnaW4iOiJFYXJ0aGRhdGEgTG9naW4iLCJzaWciOiJlZGxqd3RwdWJrZXlfb3BzIiwiYWxnIjoiUlMyNTYifQ.eyJ0eXBlIjoiVXNlciIsInVpZCI6InpoYW95dXRpbSIsImV4cCI6MTY5Mjk0ODY4OSwiaWF0IjoxNjg3NzY0Njg5LCJpc3MiOiJFYXJ0aGRhdGEgTG9naW4ifQ.8VcF6eI2baAURBcKZn1Cca9xTTt5bMxMCnF9bfMaqH6GLiDMd-j3f35aTJikF1amrkRq_Mq9L-KFEUBdkOn-Qn3BFiLsxIvKIEjtvl02mGigYExtK5trxJOi4Vm3NBeZIGBjiFdOU1kjmJl-uu9o_lnWSH7xQBQc6uEJ8zrX3Z31nnel8DiwZIv1GN5R5ElUqce38oYk7xyymfzeBx94tEUi084gwuQtwTOvAc_Xly0ZQcBidJh_UKuZKCbxBgPOmwTlHPdrjN-FofSRIXx8M8CdMomJV0h9_SGxikF1r0dV-oPYDxA40vNhNMUepYYd1iGkeIFYwZUfZ5P87upZ-g'
    # emhhb3l1dGltOmVtaGhiM2wxZEdsdFFHZHRZV2xzTG1OdmJRPT06MTYyNjQ0MTQyMTphMzhkYTcwMzc5NTg1M2NhY2QzYjY2NTU0ZWFkNzFjMGEwMTljMmJj
    for hh in hh_list:
        for vv in vv_list:
            print(f"\nstart_date: {start_date}， h{hh}v{vv}")
            print("-----------------------------------------------------------")

            if products_id in ['VNP09GA_NRT']:
                # VNP09GA_NRT
                filename = 'VNP09GA_NRT.A"+str(year)+f"{julian_day}.h{hh}v{vv}.001.*.h5'
                command = "wget -e robots=off -m -np -R .html,.tmp -nH --cut-dirs=9 " + \
                    f"\"https://ladsweb.modaps.eosdis.nasa.gov/archive/allData/{url_part}/VNP09GA_NRT.A"+str(year)+f"{julian_day}.h{hh}v{vv}.001.h5\" --header \"Authorization: Bearer {token}\" \
                        -P {dir_data_with_id.joinpath(start_date)}"

            if products_id in ['MOD09GA']:
                # MOD09GA NRT from LANCE
                # MOD09GA.A2022246.h00v08.061.2022247021401.NRT.hdf
                command = "wget -e robots=off -m -np -R .html,.tmp -nH --cut-dirs=9 " + \
                    f"\"https://nrt3.modaps.eosdis.nasa.gov/api/v2/content/archives/allData/{url_part}/{products_id}.A"+str(year)+f"{julian_day}.h{hh}v{vv}.061.NRT.hdf\" --header \"Authorization: Bearer {token}\" \
                        -P {dir_data_with_id.joinpath(start_date)}"
            if len(glob.glob(os.path.join(f"{dir_data_with_id}/{start_date}", f'{products_id}.A'+str(year)+f'{julian_day}.h{hh}v{vv}.061.NRT.hdf')))!=0:
                print('HDF file exist')
                continue
            print(command)
            save_url = f"{dir_data_with_id}/{start_date}"
            print(f"\nsaved to: {save_url}")
            # if not os.path.exists(save_url):
            os.system(command)

def convert_hdf_to_geotiff(id, start_date, dir_data, dir_tif, SOURCE):
    dir_data_with_id = dir_data.joinpath(id)
    dir_tif_with_id = dir_tif.joinpath(id)
    SOURCE = edict(SOURCE)
    format = SOURCE.format
    products_id = SOURCE.products_id
    print(format, products_id)
    fileList = glob.glob(str(dir_data_with_id.joinpath(start_date).joinpath(products_id+'*'+format))) # Search for .h5 files in current directory
    pprint(fileList)
    print(dir_tif_with_id)
    for file in fileList:
        filename = file.split('/')[-1][:-4]
        print(filename)
        if len(glob.glob(os.path.join(f"{dir_tif_with_id}/{start_date}", f"{filename.replace('MOD09GA', 'MOD09GATIF')}.tif"))) != 0:
            print('TIF file exist')
            continue
        # Open just the bands that you want to process
        desired_bands = SOURCE.BANDS
        modis_bands = rxr.open_rasterio(f"{dir_data_with_id}/{start_date}/{filename}.hdf",
                                masked=True,
                                variable=desired_bands
                        ).squeeze()

        dir_tif_with_id.joinpath(start_date).mkdir(exist_ok=True)
        print(f"{filename.replace('MOD09GA', 'MOD09GATIF')}.tif")
        modis_bands.rio.reproject("EPSG:4326").rio.to_raster(f"{dir_tif_with_id}/{start_date}/{filename.replace('MOD09GA', 'MOD09GATIF')}.tif")

def upload_in_parallel(id, start_date, asset_id, filepath='data/MOD09GATIF'):
    print(filepath, id, start_date)
    julian_day = datetime.datetime.strptime(start_date, '%Y-%m-%d').timetuple().tm_yday
    file_list = glob.glob(os.path.join(filepath, id, start_date, 'MOD09GA*'+str(julian_day)+'*.tif'))
    results = []
    with multiprocessing.Pool(processes=8) as pool:
        for file in file_list:
            result = pool.apply_async(upload_hdf, (file, asset_id))
            results.append(result)
        results = [result.get() for result in results if result is not None]

download_task = PythonOperator(
    task_id='download_task',
    python_callable=download_files,
    op_kwargs={
        'id':id,
        'start_date':(datetime.datetime.today()-datetime.timedelta(days=1)).strftime('%Y-%m-%d'),
        'dir_data':dir_data,
        'dir_tif':dir_tif,
        'collection_id':collection_id,
        'products_id':products_id,
        'hh_list':hh_list,
        'vv_list':vv_list
    },
    dag=dag,
)

convert_hdf_to_geotiff_task = PythonOperator(
    task_id='convert_hdf_to_geotiff',
    python_callable=convert_hdf_to_geotiff,
    op_kwargs={
        'id':id,
        'start_date':(datetime.datetime.today()-datetime.timedelta(days=1)).strftime('%Y-%m-%d'),
        'dir_data': dir_data,
        'dir_tif': dir_tif,
        'SOURCE': SOURCE
    },
    dag=dag,
)
upload_gee_task = PythonOperator(
    task_id='upload_gee_task',
    python_callable=upload_in_parallel,
    op_kwargs={
        'id':id,
        'start_date': (datetime.datetime.today() - datetime.timedelta(days=1)).strftime('%Y-%m-%d'),
        'asset_id': asset_id
    },
    dag=dag,
)

download_task >> convert_hdf_to_geotiff_task >> upload_gee_task
