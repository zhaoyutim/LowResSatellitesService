import glob
import os
os.chdir('/geoinfo_vol1/home/z/h/zhao2/LowResSatellitesService/')
import sys
sys.path.insert(0,'/geoinfo_vol1/home/z/h/zhao2/LowResSatellitesService/')


import datetime
import multiprocessing

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta
from utils.args import default_args
import datetime
from utils.utils import get_json_tasks, get_client_tasks, json_wrapper, client_wrapper, get_tasks, main_process_wrapper, \
    upload

dag = DAG(
    'VIIRS_Night_Iband_process_and_upload_CANADA',
    default_args=default_args,
    description='A DAG for processing VIIRS night Iband images and upload to gee',

)

dir_json = '/geoinfo_vol1/home/z/h/zhao2/LowResSatellitesService/data/VNPL1'
dir_nc = '/geoinfo_vol1/home/z/h/zhao2/LowResSatellitesService/data/VNPNC'
dir_tif = '/geoinfo_vol1/home/z/h/zhao2/LowResSatellitesService/data/VNPIMGTIF'
dir_subset = '/geoinfo_vol1/home/z/h/zhao2/LowResSatellitesService/data/subset'
product_id = 'IMG'
products_id = ['VNP02'+product_id, 'VNP03'+product_id]
dn = ['N']
id = 'CANADA'
collection_id = '5200'
utmzone = '4326'
roi_arg = '-170,41,-41,73'
print(dir_subset)
def download_files(id, roi_arg, start_date, end_date, dir_json, dir_nc):
    roi = [float(roi_arg.split(',')[0]), float(roi_arg.split(',')[1]), float(roi_arg.split(',')[2]),
           float(roi_arg.split(',')[3])]
    duration = datetime.datetime.strptime(end_date, '%Y-%m-%d') - datetime.datetime.strptime(start_date, '%Y-%m-%d')
    area_of_interest = 'W' + str(roi[0]) + ' ' + 'N' + str(roi[3]) + ' ' + 'E' + str(roi[2]) + ' ' + 'S' + str(roi[1])
    print('Currently processing id {} with roi{}'.format(id, roi_arg))
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
    if not os.path.exists(os.path.join(dir_subset, id)):
        os.mkdir(os.path.join(dir_subset, id))
    tasks = get_tasks(start_date, duration, id, roi, dn, utmzone, product_id, dir_nc, dir_tif, dir_subset)

    with multiprocessing.Pool(processes=4) as pool:
        results = list(pool.imap_unordered(main_process_wrapper, tasks))
    print(results)

def upload_in_parallel(id, start_date, asset_id, filepath='data/subset'):
    print(filepath, id, start_date)
    file_list = glob.glob(os.path.join(filepath, id, 'VNPIMG'+start_date+'*.tif'))
    results = []
    with multiprocessing.Pool(processes=8) as pool:
        for file in file_list:
            id = file.split('/')[-2]
            date = file.split('/')[-1][6:16]
            time = file.split('/')[-1][17:21]
            vnp_json = open(glob.glob(os.path.join('data/VNPL1', id, date, 'D', '*.json'))[0], 'rb')
            import json
            def get_name(json):
                return json.get('name').split('.')[2]
            vnp_time = list(map(get_name, json.load(vnp_json)['content']))
            if time not in vnp_time or 'IMG' not in file:
                continue
            result = pool.apply_async(upload, (file, asset_id))
            results.append(result)
        results = [result.get() for result in results if result is not None]

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
upload_gee_task = PythonOperator(
    task_id='upload_gee_task',
    python_callable=upload_in_parallel,
    op_kwargs={
        'id':id,
        'start_date': (datetime.datetime.today()-datetime.timedelta(days=1)).strftime('%Y-%m-%d'),
        'asset_id': 'projects/ee-eo4wildfire/assets/VIIRS_Iband_Night_CANADA/'
    },
    dag=dag,
)

download_task >> read_project_task >> upload_gee_task
