import glob
import os
import sys
from pathlib import Path
root_path = str(Path(__file__).resolve().parents[1]) + "/"
sys.path.insert(0,root_path)

import datetime
import billiard as multiprocessing
from airflow import DAG
from airflow.operators.python import PythonOperator
from utils.utils import get_tasks, main_process_wrapper, upload
import datetime
from utils import config
from dag_utils import *

dir_json = root_path + 'data/VNPL1'
dir_nc = root_path + 'data/VNPNC'
dir_tif = root_path + 'data/VNPIMGTIF'
dir_subset = root_path + 'data/subset'
product_id = 'IMG'
#products_id = ['VNP02'+product_id, 'VNP03'+product_id]
#collection_id = '5200'

products_id = ['VJ102'+product_id, 'VJ103'+product_id]
collection_id = '5201'

roi_arg = '-170,41,-41,73'
ids = ["CANADA","US","EU"]
roi_args =['-170,41,-41,73','-127,24,-66,50','-24,35,41,72']
dns = ['D','N']
schedule_interval = ['0 12 * * *','0 13 * * *','0 14 * * *']

def read_and_project(id, roi_arg, start_date, end_date, dir_nc, dir_tif, dir_subset, dir_json, dn):
    roi = [float(roi_arg.split(',')[0]), float(roi_arg.split(',')[1]), float(roi_arg.split(',')[2]),
           float(roi_arg.split(',')[3])]
    duration = datetime.datetime.strptime(end_date, '%Y-%m-%d') - datetime.datetime.strptime(start_date, '%Y-%m-%d')
    os.makedirs(os.path.join(dir_subset, id),exist_ok=True)
    tasks = get_tasks(start_date, duration, id, roi, dn, product_id, dir_tif)

    with multiprocessing.Pool(processes=4) as pool:
        list(pool.imap_unordered(main_process_wrapper, tasks))

def upload_in_parallel(id, start_date, end_date, asset_id, dn, filepath=root_path+'data/subset', type="IMG", dir_json=root_path+'data/VNPL1'):
    file_list = []
    duration = datetime.datetime.strptime(end_date, '%Y-%m-%d') - datetime.datetime.strptime(start_date, '%Y-%m-%d')
    for k in range(duration.days):
        date = (datetime.datetime.strptime(start_date, '%Y-%m-%d') + datetime.timedelta(k)).strftime('%Y-%m-%d')
        for i in dn:
            vnp_json = open(glob.glob(os.path.join(dir_json, id, date, i, '*.json'))[0], 'rb')
            import json
            def get_name(json):
                return json.get('name').split('.')[2]
            vnp_time = list(map(get_name, json.load(vnp_json)['content']))
            files = glob.glob(os.path.join(filepath, id, date, i,'VNP'+type+date+'*.tif'))
            for file in files:
            #    if file.split('.')[-2][-4:] not in vnp_time:
            #        print('Time {} not exist'.format(file.split('.')[-2][-4:]))
            #        continue
                file_list.append(file)
    print("Found",len(file_list),"files to upload.")
    
    results = []
    with multiprocessing.Pool(processes=8) as pool:
        for file in file_list:
            result = pool.apply_async(upload, (file, asset_id))
            results.append(result)
        results = [result.get() for result in results if result is not None]

for i in range(len(ids)):
    for dn in dns:
        if dn=='D':
            dn_name = 'Day'
        else:
            dn_name = 'Night'

        dag = DAG(
            f'VIIRS_{dn_name}_Iband_process_and_upload_{ids[i]}',
            default_args=config.default_args,
            schedule_interval=schedule_interval[i],
            description='A DAG for processing VIIRS Iband images and upload to gee',
        )

        with dag:
            download_task = PythonOperator(
                task_id='download_task',
                python_callable=download_viirs,
                op_kwargs={
                    'id':ids[i],
                    'roi_arg':roi_args[i],
                    'start_date':(datetime.datetime.today()-datetime.timedelta(days=1)).strftime('%Y-%m-%d'),
                    'end_date': datetime.datetime.today().strftime('%Y-%m-%d'),
                    'dir_json':dir_json,
                    'dir_nc':dir_nc,
                    'dn_img':[dn],
                    'products_id_img': products_id,
                    'collection_id': collection_id
                },
            )

            read_project_task = PythonOperator(
                task_id='read_project_task',
                python_callable=read_and_project,
                op_kwargs={
                    'id':ids[i],
                    'roi_arg':roi_args[i],
                    'start_date': (datetime.datetime.today()-datetime.timedelta(days=1)).strftime('%Y-%m-%d'),
                    'end_date': datetime.datetime.today().strftime('%Y-%m-%d'),
                    'dir_nc': dir_nc,
                    'dir_tif': dir_tif,
                    'dir_subset': dir_subset,
                    'dir_json': dir_json,
                    'dn': dn
                },
            )

            upload_gee_task = PythonOperator(
                task_id='upload_gee_task',
                python_callable=upload_in_parallel,
                op_kwargs={
                    'id':ids[i],
                    'start_date': (datetime.datetime.today()-datetime.timedelta(days=1)).strftime('%Y-%m-%d'),
                    'end_date': datetime.datetime.today().strftime('%Y-%m-%d'),
                    'asset_id': f'projects/ee-eo4wildfire/assets/VIIRS_Iband_{dn_name}_{ids[i]}/',
                    'dn': dn
                },
            )

            download_task >> read_project_task >> upload_gee_task
