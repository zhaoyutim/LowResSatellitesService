import glob
import os
import sys
from pathlib import Path
root_path = str(Path(__file__).resolve().parents[1]) + "/"
sys.path.insert(0,root_path)

import billiard as multiprocessing
from easydict import EasyDict as edict
import rioxarray as rxr
from airflow import DAG
from airflow.operators.python import PythonOperator
import datetime
from utils import config

from utils.utils import upload_hdf

dir_data = Path(root_path + 'data/MOD09GA')
dir_tif = Path(root_path + 'data/MOD09GATIF')

SOURCE = edict(config.modis_config['MOD09GA'])
products_id = SOURCE.products_id
collection_id = SOURCE.collection_id
start_date = (datetime.datetime.today()-datetime.timedelta(days=1)).strftime('%Y-%m-%d')
ids = ["NA","EU"]
hh_lists = [['08', '09', '10', '11', '12', '13', '14'], #NA
            ['17', '18', '19', '20', '21', '22', '23']] #EU
vv_list = ['02', '03', '04', '05']

def download_files(id, start_date, dir_data, collection_id, products_id, hh_list=['10', '11'], vv_list =['03']):
    year = start_date[:4]
    julian_day = datetime.datetime.strptime(start_date, '%Y-%m-%d').timetuple().tm_yday

    dir_data_id = dir_data.joinpath(id)
    dir_data_id.mkdir(exist_ok=True,parents=True)

    url_part = f"{collection_id}/{products_id}/{year}/{julian_day}"
    dir_data_id_date = dir_data_id.joinpath(start_date)
    dir_data_id_date.mkdir(exist_ok=True,parents=True)
    
    for hh in hh_list:
        for vv in vv_list:
            print(f"\nstart_date: {start_date}， h{hh}v{vv}")
            print("-----------------------------------------------------------")

            if products_id in ['VNP09GA_NRT']:
                # VNP09GA_NRT
                command = "wget -e --timeout=5 robots=off -m -np -R .html,.tmp -nH --cut-dirs=9 " + \
                    f"\"https://ladsweb.modaps.eosdis.nasa.gov/archive/allData/{url_part}/VNP09GA_NRT.A"+str(year)+f"{julian_day}.h{hh}v{vv}.001.h5\" --header \"Authorization: Bearer {config.auth_token}\" \
                        -P {dir_data_id_date}"

            if products_id in ['MOD09GA']:
                # MOD09GA NRT from LANCE
                # MOD09GA.A2022246.h00v08.061.2022247021401.NRT.hdf
                command = "wget -e --timeout=5 robots=off -m -np -R .html,.tmp -nH --cut-dirs=9 " + \
                    f"\"https://nrt4.modaps.eosdis.nasa.gov/api/v2/content/archives/allData/{url_part}/{products_id}.A"+str(year)+f"{julian_day}.h{hh}v{vv}.061.NRT.hdf\" --header \"Authorization: Bearer {config.auth_token}\" \
                        -P {dir_data_id_date}"
            if len(glob.glob(os.path.join(dir_data_id_date, f'{products_id}.A'+str(year)+f'{julian_day}.h{hh}v{vv}.061.NRT.hdf')))!=0:
                print('HDF file exist')
                continue
            print(command)
            print(f"\nsaved to: {dir_data_id_date}")
            # if not os.path.exists(save_url):
            os.system(command)

def convert_hdf_to_geotiff(id, start_date, dir_data, dir_tif, SOURCE):
    dir_data_with_id = dir_data.joinpath(id)
    dir_tif_with_id = dir_tif.joinpath(id)
    SOURCE = edict(SOURCE)
    format = SOURCE.format
    products_id = SOURCE.products_id
    fileList = glob.glob(str(dir_data_with_id.joinpath(start_date).joinpath(products_id+'*'+format))) # Search for .h5 files in current directory
    print("ATTEMPTING CONVERT HDF TO GEOTIFF", len(fileList))
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

        dir_tif_with_id.joinpath(start_date).mkdir(exist_ok=True,parents=True)
        print(f"{filename.replace('MOD09GA', 'MOD09GATIF')}.tif")
        modis_bands.rio.reproject("EPSG:4326").rio.to_raster(f"{dir_tif_with_id}/{start_date}/{filename.replace('MOD09GA', 'MOD09GATIF')}.tif")

def upload_in_parallel(id, start_date, asset_id, filepath=root_path+'data/MOD09GATIF'):
    print(filepath, id, start_date)
    julian_day = datetime.datetime.strptime(start_date, '%Y-%m-%d').timetuple().tm_yday
    file_list = glob.glob(os.path.join(filepath, id, start_date, 'MOD09GA*'+str(julian_day)+'*.tif'))
    print("ATTEMPTING UPLOAD ", len(file_list))
    results = []

    with multiprocessing.Pool(processes=8) as pool:
        for file in file_list:
            result = pool.apply_async(upload_hdf, (file, asset_id))
            results.append(result)
        results = [result.get() for result in results if result is not None]


for i in range(len(ids)):
    dag = DAG(
        f'MODIS_process_and_upload_{ids[i]}',
        default_args=config.default_args,
        schedule_interval='0 10 * * *',
        description='A DAG for processing MODIS images and upload to gee',
    )
    with dag:
        download_task = PythonOperator(
            task_id='download_task',
            python_callable=download_files,
            op_kwargs={
                'id':ids[i],
                'start_date': start_date,
                'dir_data':dir_data,
                'dir_tif':dir_tif,
                'collection_id':collection_id,
                'products_id':products_id,
                'hh_list':hh_lists[i],
                'vv_list':vv_list
            }
        )

        convert_hdf_to_geotiff_task = PythonOperator(
            task_id='convert_hdf_to_geotiff',
            python_callable=convert_hdf_to_geotiff,
            op_kwargs={
                'id':ids[i],
                'start_date': start_date,
                'dir_data': dir_data,
                'dir_tif': dir_tif,
                'SOURCE': SOURCE
            }
        )
        upload_gee_task = PythonOperator(
            task_id='upload_gee_task',
            python_callable=upload_in_parallel,
            op_kwargs={
                'id':ids[i],
                'start_date': start_date,
                'asset_id': f'projects/ee-eo4wildfire/assets/MODIS_{ids[i]}/'
            }
        )

        download_task >> convert_hdf_to_geotiff_task >> upload_gee_task
