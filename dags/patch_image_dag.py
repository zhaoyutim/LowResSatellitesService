import glob
import os
import sys
from pathlib import Path
root_path = str(Path(__file__).resolve().parents[1]) + "/"
sys.path.insert(0,root_path)

import datetime
import billiard as multiprocessing
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
import skimage

dag = DAG(
    'patch_image_dag',
    default_args=config.default_args,
    schedule_interval='0 10 * * *',
    description='A DAG for processing VIIRS Iband images and upload to gee for US',
)

dir_json = root_path + 'data/mosaic/json'
dir_nc = root_path + 'data/mosaic/nc'
dir_tif = root_path + 'data/mosaic/tif'
dir_subset = root_path + 'data/mosaic/subset'
products_id_img = ['VNP02IMG', 'VNP03IMG'] 
products_id_mod= ['VNP02MOD','VNP03MOD']
dn_img = ['D','N','B']
dn_mod = ['D']
id = 'MOSAIC'
collection_id = '5200'
utmzone = '4326'
roi_arg = '-105,00,-100,05'
start_date = (datetime.datetime.today()-datetime.timedelta(days=2*3)).strftime('%Y-%m-%d')
end_date = datetime.datetime.today().strftime('%Y-%m-%d')
interval = 2

def download_files(id, roi_arg, start_date, end_date, dir_json, dir_nc):
    roi = [float(roi_arg.split(',')[0]), float(roi_arg.split(',')[1]), float(roi_arg.split(',')[2]),
           float(roi_arg.split(',')[3])]
    duration = datetime.datetime.strptime(end_date, '%Y-%m-%d') - datetime.datetime.strptime(start_date, '%Y-%m-%d')
    area_of_interest = 'W' + str(roi[0]) + ' ' + 'N' + str(roi[3]) + ' ' + 'E' + str(roi[2]) + ' ' + 'S' + str(roi[1])
    print('Currently processing id {}'.format(id))

    tasks_img = get_json_tasks(id, start_date, duration, area_of_interest, products_id_img, dn_img, dir_json, collection_id)
    tasks2_img = get_client_tasks(id, start_date, duration, products_id_img, dn_img, dir_json, dir_nc, collection_id)
    with multiprocessing.Pool(processes=4) as pool:
        results = list(pool.imap_unordered(json_wrapper, tasks_img))

    with multiprocessing.Pool(processes=4) as pool:
        results = list(pool.imap_unordered(client_wrapper, tasks2_img))

    tasks_mod = get_json_tasks(id, start_date, duration, area_of_interest, products_id_mod, dn_mod, dir_json, collection_id)
    tasks2_mod = get_client_tasks(id, start_date, duration, products_id_mod, dn_mod, dir_json, dir_nc, collection_id)
    with multiprocessing.Pool(processes=4) as pool:
        results = list(pool.imap_unordered(json_wrapper, tasks_mod))

    with multiprocessing.Pool(processes=4) as pool:
        results = list(pool.imap_unordered(client_wrapper, tasks2_mod))

def read_and_project(id, roi_arg, start_date, end_date, dir_nc, dir_tif, dir_subset):
    roi = [float(roi_arg.split(',')[0]), float(roi_arg.split(',')[1]), float(roi_arg.split(',')[2]),
           float(roi_arg.split(',')[3])]
    duration = datetime.datetime.strptime(end_date, '%Y-%m-%d') - datetime.datetime.strptime(start_date, '%Y-%m-%d')
    os.makedirs(os.path.join(dir_subset, id),exist_ok=True)
    tasks_img = get_tasks(start_date, duration, id, roi, dn_img, utmzone, 'IMG', dir_nc, dir_tif, dir_subset, dir_json)
    tasks_mod = get_tasks(start_date, duration, id, roi, dn_mod, utmzone, 'MOD', dir_nc, dir_tif, dir_subset, dir_json)

    with multiprocessing.Pool(processes=4) as pool:
        results = list(pool.imap_unordered(main_process_wrapper, tasks_img))
    print(results)

    with multiprocessing.Pool(processes=4) as pool:
        results = list(pool.imap_unordered(main_process_wrapper, tasks_mod))
    print(results)

def to_mosaic(id,start_date,end_date):    
    duration = datetime.datetime.strptime(end_date, '%Y-%m-%d') - datetime.datetime.strptime(start_date, '%Y-%m-%d')
    mosaic_day_paths = []
    for k in range(duration.days):
        date = (datetime.datetime.strptime(start_date, '%Y-%m-%d') + datetime.timedelta(k)).strftime('%Y-%m-%d')
        os.makedirs(root_path+'data/mosaic/daily/'+date,exist_ok=True)
        mosaic_day_paths.append(root_path+'data/mosaic/daily/'+date+'/VNP'+date+'_mosaic.tif')
        print('Processing: ' + date)
        
        img_day_tiff_files = glob.glob(os.path.join(dir_subset, id, date, 'D')+'/**/VNPIMG*.tif', recursive=True)
        img_night_tiff_files = glob.glob(os.path.join(dir_subset, id, date, 'N')+'/**/VNPIMG*.tif', recursive=True)
        mod_tiff_files = glob.glob(os.path.join(dir_subset, id, date)+'/**/VNPMOD*.tif', recursive=True)
        tiff_files =[img_day_tiff_files,img_night_tiff_files,mod_tiff_files]
        channel_names=['D','BN','MOD']
        print('Found ', len(img_day_tiff_files)+len(img_night_tiff_files)+len(mod_tiff_files), ' files')
        print('Remove Nan')

        for channel in range(3):
            for file in tiff_files[channel]:
                print("Reading file: ", file)
                array, profile = main_mosaic.read_tiff(file)
                array = np.nan_to_num(array)
                main_mosaic.write_tiff(file, array, profile)
            print('Finish remove Nan')

            mosaic, mosaic_metadata = main_mosaic.mosaic_geotiffs(tiff_files[channel])
            os.makedirs(os.path.join(root_path+'data/mosaic/channels',date,channel_names[channel]),exist_ok=True)
            output_path = os.path.join(root_path+'data/mosaic/channels',date, channel_names[channel], 'VNP'+date+'_mosaic.tif')
            main_mosaic.write_tiff(output_path, mosaic, mosaic_metadata)
            print("Created mosaic for ", channel_names[channel])
        
        main_mosaic.combine_tiff([os.path.join(root_path+'data/mosaic/channels', date, channel_names[0], 'VNP'+date+'_mosaic.tif'),
                                  os.path.join(root_path+'data/mosaic/channels', date, channel_names[1], 'VNP'+date+'_mosaic.tif'),
                                  os.path.join(root_path+'data/mosaic/channels', date, channel_names[2], 'VNP'+date+'_mosaic.tif')], mosaic_day_paths[k])
        print('Finish Creating mosaic ', k)

def patch_images(id, start_date, end_date, dir_mosaics, interval):
    duration = datetime.datetime.strptime(end_date, '%Y-%m-%d') - datetime.datetime.strptime(start_date, '%Y-%m-%d')
    os.makedirs(root_path+'data/mosaic/output',exist_ok=True)
    for i in range(duration.days//interval):
        date = (datetime.datetime.strptime(start_date, '%Y-%m-%d') + datetime.timedelta(i*interval)).strftime('%Y-%m-%d')
        from_date = date
        path = os.path.join(dir_mosaics, date, 'VNP'+date+'_mosaic.tif')
        tif, _ = main_mosaic.read_tiff(path)
        image = np.array(tif)
        patched_image = patch_image(image)

        for j in range(interval-1):
            date = (datetime.datetime.strptime(start_date, '%Y-%m-%d') + datetime.timedelta(i*interval+j+1)).strftime('%Y-%m-%d')
            print(date)
            path = os.path.join(dir_mosaics, date, 'VNP'+date+'_mosaic.tif')
            tif, _ = main_mosaic.read_tiff(path)
            image2 = np.array(tif)
            patched_image2 = patch_image(image2)

            patched_image = np.concatenate((patched_image, patched_image2),axis=2)

        to_date = (datetime.datetime.strptime(start_date, '%Y-%m-%d') + datetime.timedelta(i*interval+j+1)).strftime('%Y-%m-%d')
        np.save(root_path+'data/mosaic/output/batched_patches_'+from_date+'-'+to_date,patched_image)

def patch_image(img):
    shape = img.shape
    patched_image = flatten(skimage.util.view_as_windows(img[:,:shape[1]//128*128,:shape[2]//128*128], window_shape = (8, 256, 256), step = 128).squeeze(),3)
    patched_image = patched_image.reshape(-1, *patched_image.shape[-3:])

    if shape[1]//128*128 != shape[1]:
        patched_edge = flatten(skimage.util.view_as_windows(img[:,shape[1]-256:shape[1],:shape[2]//128*128], window_shape = (8, 256, 256), step = 128).squeeze(),3)
        patched_image = np.concatenate((patched_image, patched_edge),axis=0)

    if shape[2]//128*128 != shape[2]:
        patched_edge = flatten(skimage.util.view_as_windows(img[:,:shape[1]//128*128,shape[2]-256:shape[2]], window_shape = (8, 256, 256), step = 128).squeeze(),3)
        patched_image = np.concatenate((patched_image, patched_edge),axis=0)

    if shape[1]//128*128 != shape[1] and shape[2]//128*128 != shape[2]:
        patched_image = np.concatenate((patched_image, np.expand_dims(img[:,shape[1]-256:shape[1],shape[2]-256:shape[2]],axis=0)),axis=0)

    return np.expand_dims(patched_image,axis=2)

def flatten(array,except_last_rows):
    return array.reshape(-1, *array.shape[-except_last_rows:])

download_task = PythonOperator(
    task_id='download_task',
    python_callable=download_files,
    op_kwargs={
        'id':id,
        'roi_arg':roi_arg,
        'start_date': start_date,
        'end_date': end_date,
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
        'start_date': start_date,
        'end_date': end_date,
        'dir_nc': dir_nc,
        'dir_tif': dir_tif,
        'dir_subset': dir_subset,
        'dir_json': dir_json
    },
    dag=dag,
)

to_mosaic_task = PythonOperator(
    task_id='to_mosaic_task',
    python_callable=to_mosaic,
    op_kwargs={
        'id':id,
        'start_date': start_date,
        'end_date': end_date
    },
    dag=dag,
)

patch_images_task = PythonOperator(
    task_id='patch_images_task',
    python_callable=patch_images,
    op_kwargs={
        'id':id,
        'start_date': start_date,
        'end_date': end_date,
        'dir_mosaics': root_path+'data/mosaic/daily',
        'interval': interval
    },
    dag=dag,
)

download_task >> read_project_task >> to_mosaic_task >> patch_images_task