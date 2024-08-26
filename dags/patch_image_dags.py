import os
import sys
from pathlib import Path
root_path = str(Path(__file__).resolve().parents[1]) + "/"
sys.path.insert(0,root_path)

import billiard as multiprocessing
from airflow import DAG
from airflow.operators.python import PythonOperator
from utils.utils import *
import datetime
from utils import config
import main_mosaic
import numpy as np
from dag_utils import *

dir_json = root_path + 'data/VNPL1'
dir_nc = root_path + 'data/VNPNC'
dir_tif = root_path + 'data/VNPIMGTIF'
dir_subset = root_path + 'data/subset'
products_id_img = ["VJ102IMG","VJ103IMG"] #['VNP02IMG', 'VNP03IMG']
products_id_mod= ['VJ102MOD','VJ103MOD'] #['VNP02MOD','VNP03MOD']
dn_img = ['D','N','B']
dn_mod = ['D']
collection_id = '5201' #'5200'

ids = ["CANADA","US","EU"]
roi_args =['-170,41,-41,73','-127,24,-66,50','-24,35,41,72']

start_date = (datetime.datetime.today()-datetime.timedelta(days=4)).strftime('%Y-%m-%d')
end_date = (datetime.datetime.today()-datetime.timedelta(days=2)).strftime('%Y-%m-%d')
interval = 2
schedule_interval = ['0 12 * * *','0 14 * * *','0 16 * * *']

def patch_region(id, roi_arg, start_date, end_date):
    return
    patch_region_tasks = get_patch_region_tasks(id, start_date, end_date, roi_arg)
    print("LENGTH",len(patch_region_tasks))
    with multiprocessing.Pool(processes=16) as pool:
        list(pool.imap_unordered(patch_region_wrapper, patch_region_tasks))

def read_and_project(id, roi_arg, start_date, end_date, dir_tif):
    return
    roi = [float(roi_arg.split(',')[0]), float(roi_arg.split(',')[1]), float(roi_arg.split(',')[2]),
           float(roi_arg.split(',')[3])]
    duration = datetime.datetime.strptime(end_date, '%Y-%m-%d') - datetime.datetime.strptime(start_date, '%Y-%m-%d')
    tasks = get_tasks(start_date, duration, id, roi, dn_img, 'IMG', dir_tif)
    tasks.extend(get_tasks(start_date, duration, id, roi, dn_mod, 'MOD', dir_tif))
    with multiprocessing.Pool(processes=6) as pool:
        list(pool.imap_unordered(main_process_wrapper, tasks))

def patch_images(id, start_date, end_date, dir_mosaics, interval, roi_arg, step):
    print("Debug 1")
    roi = [float(roi_arg.split(',')[0]), float(roi_arg.split(',')[1]), float(roi_arg.split(',')[2]),
           float(roi_arg.split(',')[3])]
    tasks = get_patch_images_tasks(id, start_date, end_date, roi, step, interval, dir_mosaics)
    print("Debug 2")
    with multiprocessing.Pool(processes=16) as pool:
        list(pool.imap_unordered(patch_image_wrapper, tasks))

def patch_images2(id, start_date, end_date, dir_mosaics, interval, roi_arg):
    duration = datetime.datetime.strptime(end_date, '%Y-%m-%d') - datetime.datetime.strptime(start_date, '%Y-%m-%d')
    os.makedirs(os.path.join(root_path, 'data/mosaics/batched_patches',id),exist_ok=True)

    step = 5-10*64/1488
    roi = [float(roi_arg.split(',')[0]), float(roi_arg.split(',')[1]), float(roi_arg.split(',')[2]),
           float(roi_arg.split(',')[3])]
    x_start, x_stop = np.arange(roi[0],roi[2],step), np.arange(roi[0]+5,roi[2]+5,step)
    y_start, y_stop = np.arange(roi[1],roi[3],step), np.arange(roi[1]+5,roi[3]+5,step)

    for i in range(len(x_start)):
        for j in range(len(y_start)):            
                roi_string = '_'.join(str(x) for x in np.array([x_start[i], y_start[j], x_stop[i], y_stop[j]]))

                for l in range(duration.days//interval):
                    date = (datetime.datetime.strptime(start_date, '%Y-%m-%d') + datetime.timedelta(l*interval)).strftime('%Y-%m-%d')
                    from_date = date
                    path = os.path.join(dir_mosaics,id,date,'VNP'+roi_string+'.tif')
                    
                    tif, _ = main_mosaic.read_tiff(path)
                    image = np.array(tif)
                    patched_image = patch_image(image)

                    for k in range(interval-1):
                        date = (datetime.datetime.strptime(start_date, '%Y-%m-%d') + datetime.timedelta(l*interval+k+1)).strftime('%Y-%m-%d')

                        path = os.path.join(dir_mosaics,id,date, 'VNP'+roi_string+'.tif')
                        tif, _ = main_mosaic.read_tiff(path)
                        image2 = np.array(tif)
                        patched_image2 = patch_image(image2)

                        patched_image = np.concatenate((patched_image, patched_image2),axis=2)

                    to_date = (datetime.datetime.strptime(start_date, '%Y-%m-%d') + datetime.timedelta(l*interval+k+1)).strftime('%Y-%m-%d')
                    np.save(os.path.join(root_path, 'data/mosaics/batched_patches',id,from_date+'-'+to_date+'_'+roi_string),patched_image)
                    print("Saved batched pathches", os.path.join(root_path, 'data/mosaics/batched_patches',id,from_date+'-'+to_date+'_'+roi_string))


def patch_image(img):
    patched_image = np.zeros(shape=(11,11,8,256,256))
    for i in range(1488//128-1):
        for j in range(1488//128-1):
            patched_image[i,j,:,:,:] = img[:,128*i:128*(i+2),128*j:128*(j+2)]
    for j in range(1488//128-1):
        patched_image[10,j,:,:,:] = img[:,1488-256:,128*j:128*(j+2)]
    for i in range(1488//128-1):
        patched_image[i,10,:,:,:] = img[:,128*i:128*(i+2),1488-256:]
    patched_image[10,10,:,:,:] = img[:,1488-256:,1488-256:]
    return np.expand_dims(flatten(patched_image,3),axis=2)

def flatten(array,except_last_rows):
    return array.reshape(-1, *array.shape[-except_last_rows:])

def to_mosaic(id,start_date,end_date,roi_arg):
    return
    roi = [float(roi_arg.split(',')[0]), float(roi_arg.split(',')[1]), float(roi_arg.split(',')[2]),
        float(roi_arg.split(',')[3])]

    step = 5-10*64/1488
    x_start, x_stop = np.arange(roi[0],roi[2],step), np.arange(roi[0]+5,roi[2]+5,step)
    y_start, y_stop = np.arange(roi[1],roi[3],step), np.arange(roi[1]+5,roi[3]+5,step)
    rois = np.zeros(shape=(x_start.shape[0],y_start.shape[0],4))
    duration = datetime.datetime.strptime(end_date, '%Y-%m-%d') - datetime.datetime.strptime(start_date, '%Y-%m-%d')
    for i in range(len(x_start)):
        for j in range(len(y_start)):            
                rois[i,j,:]=np.array([x_start[i], y_start[j], x_stop[i], y_stop[j]])
    
    mosaic_tasks = get_mosaic_tasks(id, start_date,end_date,rois,root_path+"data/patched_regions")
    with multiprocessing.Pool(processes=16) as pool:
        list(pool.imap_unordered(mosaic_wrapper, mosaic_tasks))
    #os.remove('/home/a/a/aadelow/LowResSatellitesService/data/mosaics/channels')

for i in range(len(ids)):
    dag = DAG(
        'patch_image_dag_'+ids[i],
        default_args=config.default_args,
        schedule_interval=schedule_interval[i],
        description='A DAG for processing VIIRS Iband images and upload to gee for US',
    )

    with dag:
        download_task = PythonOperator(
            task_id='download_task',
            python_callable=download_viirs,
            op_kwargs={
                'id':ids[i],
                'roi_arg':roi_args[i],
                'start_date': start_date,
                'end_date': end_date,
                'dir_json':dir_json,
                'dir_nc':dir_nc,
                'dn_img':['D','N','B'],
                'dn_mod':['D'],
                'collection_id': collection_id,
                'products_id_img': products_id_img,
                'products_id_mod': products_id_mod
            },
        )

        read_project_task = PythonOperator(
            task_id='read_project_task',
            python_callable=read_and_project,
            op_kwargs={
                'id':ids[i],
                'roi_arg':roi_args[i],
                'start_date': start_date,
                'end_date': end_date,
                'dir_tif': dir_tif,
            },
        )

        patch_region_task = PythonOperator(
            task_id='patch_region_task',
            python_callable=patch_region,
            op_kwargs={
                'id': ids[i],
                'roi_arg': roi_args[i],
                'start_date': start_date,
                'end_date': end_date
            },
        )

        to_mosaic_task = PythonOperator(
            task_id='to_mosaic_task',
            python_callable=to_mosaic,
            op_kwargs={
                'id':ids[i],
                'start_date': start_date,
                'end_date': end_date,
                'roi_arg': roi_args[i]
            },
        )
        
        patch_images_task = PythonOperator(
            task_id='patch_images_task',
            python_callable=patch_images,
            op_kwargs={
                'id':ids[i],
                'start_date': start_date,
                'end_date': end_date,
                'dir_mosaics': root_path+'data/mosaics',
                'interval': interval,
                'roi_arg': roi_args[i],
                'step': 5-10*64/1488
            }
        )

        download_task >> read_project_task >> patch_region_task >> to_mosaic_task >> patch_images_task