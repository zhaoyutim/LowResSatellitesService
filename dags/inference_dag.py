import glob
import os
import sys
from pathlib import Path
root_path = str(Path(__file__).resolve().parents[1]) + "/"
sys.path.insert(0,root_path)

import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta
import datetime
from utils import config
import main_mosaic
import numpy as np
import subprocess
import utils
import rasterio
import infer

dag = DAG(
    'inference_dag',
    default_args=config.default_args,
    schedule_interval='0 10 * * *',
    description='A DAG for processing VIIRS Iband images and upload to gee for US',
)

dir_tif = root_path + 'data/mosaic/daily'
products_id_img = ['VNP02IMG', 'VNP03IMG'] 
products_id_mod= ['VNP02MOD','VNP03MOD']
dn_img = ['D','N','B']
dn_mod = ['D']

collection_id = '5200'
utmzone = '4326'
roi_arg = '-125,55,-120,60'

start_date = (datetime.datetime.today()-datetime.timedelta(days=3)).strftime('%Y-%m-%d')
end_date = (datetime.datetime.today()-datetime.timedelta(days=2)).strftime('%Y-%m-%d')
interval = 2
dir_data = root_path + 'model_outputs'
asset_id = 'projects/ee-eo4wildfire/assets/swinunetr3d'

def inference(id, roi_arg, start_date, end_date, dir_data):
    print("Starting inference...")
    infer.run('swinunetr3d','af',4,3,36,8,2,'v1',str(start_date),str(end_date), 
              '/home/a/a/aadelow/LowResSatellitesService/model_outputs',
              '/home/a/a/aadelow/LowResSatellitesService/data/mosaic/output',
              '/home/a/a/aadelow/TS-SatFire/saved_models/model_swinunetr3d_mode_af_num_heads_3_hidden_size_36_batchsize_4_checkpoint_epoch_80_nc_8_ts_2.pth')
    print("Inference completed.")

def reconstruct_image(id, roi_arg, start_date, end_date, dir_tif, dir_data):
    print('Reconstructing...')
    dates_list = list(np.arange(np.datetime64(start_date), np.datetime64(end_date) + np.timedelta64(1, 'D')))
    print(dates_list)
    paths = glob.glob(dir_data+'/*'+ start_date + '-' + end_date +'.npy')
    print('Found ',len(paths),' files.')
    reconstructed_image = np.zeros(shape=(2,1488-128,1488-128))
    roi = list(np.float_(roi_arg.split(',')))
    for path in paths:
        data = np.load(path)[:,:,64:128+64,64:128+64]
        print(data.shape)
        data = np.reshape(data, (10, 10, 2, 128, 128))
        for i in range(1488//128-2):
            for j in range(1488//128-2):
                reconstructed_image[:,128*i:128*(i+1),128*j:128*(j+1)] += data[i,j,:,:,:]
        for j in range(1488//128-2):
            reconstructed_image[:,1488-2*128:,128*j:128*(j+1)]+=data[9,j,:,:,:]
        for i in range(1488//128-2):
            reconstructed_image[:,128*i:128*(i+1),1488-2*128:]+=data[i,9,:,:,:]
        reconstructed_image[:,1488-2*128:,1488-2*128:]+=data[9,9,:,:,:]
        reconstructed_image = reconstructed_image>0

        for i in range(len(dates_list)):
            print("Saving .tif with", np.sum(reconstructed_image[i]==1), "active fire pixels detected.")
            image = np.expand_dims(reconstructed_image[i,:,:],axis=0)
            tif_file = glob.glob(dir_tif+'/*'+str(dates_list[i])+'.tif')[0]
            _, metadata = main_mosaic.read_tiff(tif_file)
            diff = 5*64/1488
            transform = rasterio.transform.from_bounds(roi[0]+diff, 
                                           roi[1]+diff, 
                                           roi[2]-diff, 
                                           roi[3]-diff,
                                           width=image.shape[1], 
                                           height=image.shape[2])
            metadata.update({
                "count": 1,
                "width": image.shape[1],
                "height": image.shape[2],
                "transform": transform
            })
            print("Reconstructed image to shape ", image.shape)
            print(metadata)
            print("min", np.min(image),"max",np.max(image))
            main_mosaic.write_tiff(root_path + 'data/mosaic/inferred/swinunetr3d'+str(dates_list[i])+".tif",image,metadata)

def upload_in_parallel(id, start_date, end_date, asset_id, dir_tif):
    dates_list = list(np.arange(np.datetime64(start_date), np.datetime64(end_date) + np.timedelta64(1, 'D')))
    for date in dates_list:
        path = glob.glob(os.path.join(dir_tif, '*'+str(date)+'.tif'))[0]
        upload_to_gcloud(path)
        upload_to_gee(path,date,asset_id=asset_id)

def upload_to_gcloud(file, gs_path='gs://ai4wildfire/VNPPROJ5/'):
    print('Upload to gcloud')

    file_name = file.split('/')[-1]
    id = 'swinunetr3d'
    gs_path += id + '/' + file_name
    upload_cmd = 'gsutil cp ' + file + ' '+gs_path
    print(upload_cmd)
    os.system(upload_cmd)
    print('finish uploading' + file_name)

def upload_to_gee(file, date, gs_path='gs://ai4wildfire/VNPPROJ5/', asset_id='projects/proj5-dataset/assets/proj5_dataset/'):
    print('start uploading to gee')
    file_name = file.split('/')[-1]
    print(file_name)
    id = '0000'
    gs_path += 'swinunetr3d' + '/' + file_name
    time_start = str(date) + 'T' + '00' + ':' + '00' + ':00'
    cmd = utils.config.ee_path + ' upload image --force --time_start ' + time_start + ' --asset_id='+asset_id + '/' + str(date) + ' --pyramiding_policy=sample '+gs_path
    print(cmd)
    subprocess.call(cmd.split())
    print('Uploading in progress for image ' + time_start)

inference_task = PythonOperator(
    task_id='inference_task',
    python_callable=inference,
    op_kwargs={
        'id':id,
        'roi_arg':roi_arg,
        'start_date': start_date,
        'end_date': end_date,
        'dir_data':dir_data
    },
    dag=dag,
)

reconstruct_image_task = PythonOperator(
    task_id='reconstruct_image_task',
    python_callable=reconstruct_image,
    op_kwargs={
        'id':id,
        'roi_arg':roi_arg,
        'start_date': start_date,
        'end_date': end_date,
        'dir_tif': dir_tif,
        'dir_data': dir_data
    },
    dag=dag,
)

upload_task = PythonOperator(
    task_id='upload_task',
    python_callable=upload_in_parallel,
    op_kwargs={
        'id':id,
        'start_date': start_date,
        'end_date': end_date,
        'asset_id': asset_id,
        'dir_tif': root_path + 'data/mosaic/inferred'
    },
    dag=dag,
)

inference_task >> reconstruct_image_task >> upload_task