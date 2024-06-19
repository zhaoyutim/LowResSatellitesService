import glob
import os
import sys
from pathlib import Path
root_path = str(Path(__file__).resolve().parents[1]) + "/"
sys.path.insert(0,root_path)

import datetime
import multiprocessing
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from datetime import timedelta
from utils.utils import get_json_tasks, get_client_tasks, json_wrapper, client_wrapper, get_tasks, main_process_wrapper, \
    upload
import datetime
from utils import config
import main_mosaic
import numpy as np

dag = DAG(
    'active_fire_test',
    default_args=config.default_args,
    schedule_interval='0 10 * * *',
    description='A DAG for processing VIIRS Iband images and upload to gee for US',
)

dir_json = root_path + 'data/VNPL1'
dir_nc = root_path + 'data/VNPNC'
dir_tif = root_path + 'data/VNPIMGTIF'
dir_subset = root_path + 'data/subset'
product_id = 'IMG'
products_id = ['VNP02'+product_id, 'VNP03'+product_id]
dn = ['D']
id = 'US'
collection_id = '5200'
utmzone = '4326'
roi_arg = '-127,24,-66,50'

def download_files(id, roi_arg, start_date, end_date, dir_json, dir_nc):
    roi = [float(roi_arg.split(',')[0]), float(roi_arg.split(',')[1]), float(roi_arg.split(',')[2]),
           float(roi_arg.split(',')[3])]
    duration = datetime.datetime.strptime(end_date, '%Y-%m-%d') - datetime.datetime.strptime(start_date, '%Y-%m-%d')
    area_of_interest = 'W' + str(roi[0]) + ' ' + 'N' + str(roi[3]) + ' ' + 'E' + str(roi[2]) + ' ' + 'S' + str(roi[1])
    print('Currently processing id {}'.format(id))
    tasks = get_json_tasks(id, start_date, duration, area_of_interest, products_id, dn, dir_json, collection_id)
    tasks2 = get_client_tasks(id, start_date, duration, products_id, dn, dir_json, dir_nc, collection_id)
    with multiprocessing.Pool(processes=4) as pool:
        results = list(pool.imap_unordered(json_wrapper, tasks))
    with multiprocessing.Pool(processes=4) as pool:
        results = list(pool.imap_unordered(client_wrapper, tasks2))

def read_and_project(id, roi_arg, start_date, end_date, dir_nc, dir_tif, dir_subset):
    roi = [float(roi_arg.split(',')[0]), float(roi_arg.split(',')[1]), float(roi_arg.split(',')[2]),
           float(roi_arg.split(',')[3])]
    duration = datetime.datetime.strptime(end_date, '%Y-%m-%d') - datetime.datetime.strptime(start_date, '%Y-%m-%d')
    os.makedirs(os.path.join(dir_subset, id),exist_ok=True)
    tasks = get_tasks(start_date, duration, id, roi, dn, utmzone, product_id, dir_nc, dir_tif, dir_subset)

    with multiprocessing.Pool(processes=4) as pool:
        results = list(pool.imap_unordered(main_process_wrapper, tasks))
    print(results)

def to_mosaic(id,start_date,end_date,):    
    duration = datetime.datetime.strptime(end_date, '%Y-%m-%d') - datetime.datetime.strptime(start_date, '%Y-%m-%d')
    for k in range(duration.days):
        date = (datetime.datetime.strptime(start_date, '%Y-%m-%d') + datetime.timedelta(k)).strftime('%Y-%m-%d')
        print('Processing: ' + date)
        output_path = os.path.join('data/tif_dataset', id, 'VNPIMG'+date+'_mosaic.tif')
        tiff_files = glob.glob(os.path.join('data/subset/', id, 'VNPIMG'+date+'*.tif'))
        print('Remove Nan')
        for tiff_file in tiff_files:
            array, profile = main_mosaic.read_tiff(tiff_file)
            array = np.nan_to_num(array)
            #plt.imshow(array[0,:,:])
            #plt.show()
            main_mosaic.write_tiff(tiff_file, array, profile)
        print('Finish remove Nan')
        mosaic, mosaic_metadata = main_mosaic.mosaic_geotiffs(tiff_files)
        main_mosaic.write_tiff(output_path, mosaic, mosaic_metadata)
        print('Finish Creating mosaic')

download_task = PythonOperator(
    task_id='download_task',
    python_callable=download_files,
    op_kwargs={
        'id':id,
        'roi_arg':roi_arg,
        'start_date':(datetime.datetime.today()-datetime.timedelta(days=1)).strftime('%Y-%m-%d'),
        'end_date': datetime.datetime.today().strftime('%Y-%m-%d'),
        'dir_json':dir_json,
        'dir_nc':dir_nc
    },
    dag=dag,
)

read_project_task = PythonOperator(
    task_id='read_project_task',
    python_callable=read_and_project,
    op_kwargs={
        'id':id,
        'roi_arg':roi_arg,
        'start_date': (datetime.datetime.today()-datetime.timedelta(days=1)).strftime('%Y-%m-%d'),
        'end_date': datetime.datetime.today().strftime('%Y-%m-%d'),
        'dir_nc': dir_nc,
        'dir_tif': dir_tif,
        'dir_subset': dir_subset
    },
    dag=dag,
)

to_mosaic_task = PythonOperator(
    task_id='to_mosaic_task',
    python_callable=to_mosaic,
    op_kwargs={
        'id':id,
        'start_date': (datetime.datetime.today()-datetime.timedelta(days=1)).strftime('%Y-%m-%d'),
        'end_date': datetime.datetime.today().strftime('%Y-%m-%d')
    },
    dag=dag,
)

download_task >> read_project_task >> to_mosaic_task
