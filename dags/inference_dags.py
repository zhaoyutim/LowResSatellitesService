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
import billiard as multiprocessing


products_id_img = ['VNP02IMG', 'VNP03IMG'] 
products_id_mod= ['VNP02MOD','VNP03MOD']
dn_img = ['D','N','B']
dn_mod = ['D']

collection_id = '5200'
utmzone = '4326'

start_date = (datetime.datetime.today()-datetime.timedelta(days=4)).strftime('%Y-%m-%d')
end_date = (datetime.datetime.today()-datetime.timedelta(days=2)).strftime('%Y-%m-%d')
dir_data = root_path + 'model_outputs'

ids = ["CANADA","US","EU"]
models = ["swinunetr3d","unetr3d_half"]
checkpoint_paths = ["/home/a/a/aadelow/TS-SatFire/saved_models/model_swinunetr3d_mode_af_num_heads_3_hidden_size_36_batchsize_4_checkpoint_epoch_80_nc_8_ts_2.pth",
                    "/home/a/a/aadelow/TS-SatFire/saved_models/model_unetr3d_half_run_1_seed_42_mode_af_num_heads_3_hidden_size_36_batchsize_4_checkpoint_epoch_79_nc_8_ts_6_attention_v1_seed_42.pth"]
intervals = [2,6]
schedule_interval = ['0 14 * * *','0 16 * * *','0 18 * * *']

def inference(id, model_name, start_date, end_date, checkpoint_path, ts_len):
    print("Starting inference...")
    
    data_path = os.path.join(root_path, "data/mosaics/batched_patches", id)
    output_path = os.path.join(root_path, "data/model_outputs", id)
    os.makedirs(data_path, exist_ok=True)
    os.makedirs(output_path, exist_ok=True)

    infer.run(model_name,'af',32,3,36,8,ts_len,'v1',str(start_date),str(end_date),output_path,data_path,checkpoint_path)
    print("Inference completed.")

def reconstruct_image(id, model_name, start_date, end_date):
    data_path = os.path.join(root_path, "data/model_outputs", id, model_name, "raw")
    save_path = os.path.join(root_path, "data/model_outputs", id, model_name, "reconstructed")
    os.makedirs(save_path, exist_ok=True)

    print('Reconstructing...')
    dates_list = list(np.arange(np.datetime64(start_date), np.datetime64(end_date)))
    print(dates_list)
    ts_len = (datetime.datetime.strptime(end_date, '%Y-%m-%d') - datetime.datetime.strptime(start_date, '%Y-%m-%d')).days
    paths = glob.glob(data_path+'/'+ start_date + '-' + end_date +'*.npy')
    print(data_path+'/'+ start_date + '-' + end_date +'*.npy')
    print('Found ',len(paths),' files.')
    for path in paths:
        reconstructed_image = np.zeros(shape=(ts_len,1488-128,1488-128))
        roi = path.split('/')[-1].split('_')[1:]
        roi[-1] = roi[-1][:-4]
        roi_float = list(np.float_(roi))
        roi_string = '_'.join(list(np.round(np.float_(roi),2).astype(str)))
        data = np.load(path)[:,:,64:128+64,64:128+64]
        print(data.shape)
        data = np.reshape(data, (11, 11, ts_len, 128, 128))
        for i in range((1488-128)//128):
            for j in range((1488-128)//128):
                reconstructed_image[:,128*i:128*(i+1),128*j:128*(j+1)] += data[i,j,:,:,:]
        for j in range((1488-128)//128):
            reconstructed_image[:,1488-2*128:,128*j:128*(j+1)]+=data[10,j,:,:,:]
        for i in range((1488-128)//128):
            reconstructed_image[:,128*i:128*(i+1),1488-2*128:]+=data[i,10,:,:,:]
        reconstructed_image[:,1488-2*128:,1488-2*128:]+=data[10,10,:,:,:]
        reconstructed_image = reconstructed_image>0

        for i in range(len(dates_list)):
            print("Saving .tif with", np.sum(reconstructed_image[i]==1), "active fire pixels detected.")
            image = np.expand_dims(reconstructed_image[i,:,:],axis=0)
            
            diff = 5*64/1488
            transform = rasterio.transform.from_bounds(roi_float[0]+diff, 
                                           roi_float[1]+diff, 
                                           roi_float[2]-diff, 
                                           roi_float[3]-diff,
                                           width=image.shape[1], 
                                           height=image.shape[2])
            metadata = {
                'driver': 'GTiff', 
                'dtype': 'float32', 
                'nodata': 0.0, 
                'width': image.shape[1], 
                'height': image.shape[2],
                'crs': rasterio.crs.CRS.from_epsg(4326),
                "count": 1,
                "transform": transform
            }

            print("Reconstructed image to shape ", image.shape)
            print("min", np.min(image),"max",np.max(image))

            main_mosaic.write_tiff(os.path.join(save_path, str(dates_list[i]) +"_"+roi_string+".tif"),image,metadata)

def upload_in_parallel(id, start_date, end_date, asset_id, dir_tif):
    paths = []
    dates_list = list(np.arange(np.datetime64(start_date), np.datetime64(end_date)))
    for date in dates_list:
        paths.extend(glob.glob(os.path.join(dir_tif, str(date)+'*.tif')))
    
    print("Found",len(paths),"files to upload.")
    
    results = []
    with multiprocessing.Pool(processes=8) as pool:
        for file in paths:
            result = pool.apply_async(upload, (file, asset_id))
            results.append(result)
        results = [result.get() for result in results if result is not None]

def upload(file, asset_id):
    upload_to_gcloud(file)
    upload_to_gee(file, asset_id=asset_id)

def upload_to_gcloud(file, gs_path='gs://ai4wildfire/VNPPROJ5/'):
    print('Upload to gcloud')

    file_name = file.split('/')[-1].replace(".","")[:-4] + '.tif'
    gs_path += 'swinunetr3d/' + file_name
    upload_cmd = 'gsutil cp ' + file + ' '+gs_path
    print(upload_cmd)
    os.system(upload_cmd)
    print('finish uploading' + file_name)

def upload_to_gee(file, gs_path='gs://ai4wildfire/VNPPROJ5/', asset_id='projects/proj5-dataset/assets/proj5_dataset/'):
    print('start uploading to gee')
    file_name = file.split('/')[-1]
    file_name = file_name.replace(".","")[:-4]

    gs_path += 'swinunetr3d' + '/' + file_name + '.tif'
    time_start = file_name[:10] + 'T' + '00' + ':' + '00' + ':00'
    cmd = utils.config.ee_path + ' upload image --force --time_start ' + time_start + ' --asset_id='+asset_id + '/' + file_name + ' --pyramiding_policy=sample '+gs_path
    print(cmd)
    subprocess.call(cmd.split())
    print('Uploading in progress for image ' + time_start)

for model in models:
    for i in range(len(ids)):
        dag = DAG(
            f"inference_dag_{model}_{ids[i]}",
            default_args=config.default_args,
            schedule_interval=schedule_interval[i],
            description='A DAG for running inference using SWINUNETR3D'
        )

        with dag:
            inference_task = PythonOperator(
                task_id='inference_task_' + model + '_' + ids[i],
                python_callable=inference,
                op_kwargs={
                    'id': ids[i],
                    'model_name': model,
                    'start_date': start_date,
                    'end_date': end_date,
                    'checkpoint_path': checkpoint_paths[models.index(model)],
                    'ts_len': intervals[models.index(model)]
                }
            )

            reconstruct_image_task = PythonOperator(
                task_id='reconstruct_image_task_' + model + '_' + ids[i],
                python_callable=reconstruct_image,
                op_kwargs={
                    'id': ids[i],
                    'model_name': model,
                    'start_date': start_date,
                    'end_date': end_date,
                }
            )

            upload_task = PythonOperator(
                task_id='upload_task_' + model + '_' + ids[i],
                python_callable=upload_in_parallel,
                op_kwargs={
                    'id': ids[i],
                    'start_date': start_date,
                    'end_date': end_date,
                    'asset_id': 'projects/ee-eo4wildfire/assets/' + model + '_' + ids[i],
                    'dir_tif': root_path + 'data/model_outputs/' + ids[i] + '/' + model + '/reconstructed'
                }
            )

            inference_task >> reconstruct_image_task >> upload_task