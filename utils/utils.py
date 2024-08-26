import datetime
import logging
import os
import subprocess
import sys
import urllib.request as Request
import zipfile
from pathlib import Path

from google.cloud import storage

from LaadsDataHandler.laads_client import LaadsClient
from ProcessingPipeline.processing_pipeline import Pipeline
import utils.config
import ee
from dags import dag_utils
import numpy as np
import glob

root_path = str(Path(__file__).resolve().parents[1]) + "/"

logger = logging.getLogger(__name__)

laads_client = LaadsClient()
pipeline = Pipeline()

import os
os.environ["GCLOUD_PROJECT"] = "ee-eo4wildfire"

ee.Authenticate()
ee.Initialize(project=utils.config.project_name)

def json_wrapper(args):
    return laads_client.query_filelist_with_date_range_and_area_of_interest(*args)

def client_wrapper(args):
    return laads_client.download_files_to_local_based_on_filelist(*args)

def mosaic_wrapper(args):
    return dag_utils.create_mosaic(*args)

def patch_region_wrapper(args):
    return dag_utils.patch_region(*args)

def patch_image_wrapper(args):
    return dag_utils.patch_image(*args)

def get_patch_images_tasks(id, start_date, end_date, roi, step, interval, dir_mosaics):
    tasks = []
    duration = datetime.datetime.strptime(end_date, '%Y-%m-%d') - datetime.datetime.strptime(start_date, '%Y-%m-%d')
    x_start, x_stop = np.arange(roi[0],roi[2],step), np.arange(roi[0]+5,roi[2]+5,step)
    y_start, y_stop = np.arange(roi[1],roi[3],step), np.arange(roi[1]+5,roi[3]+5,step)
    for i in range(len(x_start)):
        for j in range(len(y_start)):            
                roi_string = '_'.join(str(x) for x in np.array([x_start[i], y_start[j], x_stop[i], y_stop[j]]))
                batched_tasks = []
                for l in range(duration.days//interval):
                    date = (datetime.datetime.strptime(start_date, '%Y-%m-%d') + datetime.timedelta(l*interval)).strftime('%Y-%m-%d')
                    from_date = date
                    path = os.path.join(dir_mosaics,id,date,'VNP'+roi_string+'.tif')
                    batched_tasks.append(path)
                    for k in range(interval-1):
                        date = (datetime.datetime.strptime(start_date, '%Y-%m-%d') + datetime.timedelta(l*interval+k+1)).strftime('%Y-%m-%d')
                        path = os.path.join(dir_mosaics,id,date, 'VNP'+roi_string+'.tif')
                        batched_tasks.append(path)
                    to_date = (datetime.datetime.strptime(start_date, '%Y-%m-%d') + datetime.timedelta(l*interval+k+1)).strftime('%Y-%m-%d')
                    save_path = os.path.join(root_path, 'data/mosaics/batched_patches',id,from_date+'-'+to_date+'_'+roi_string)
                    batched_tasks.append(save_path)
    return tasks

def get_patch_region_tasks(id, start_date,end_date,roi_arg):
    tasks=[]

    roi = list(np.float_(roi_arg.split(',')))
    
    step = 5-10*64/1488
    x_start, x_stop = np.arange(roi[0],roi[2],step), np.arange(roi[0]+5,roi[2]+5,step)
    y_start, y_stop = np.arange(roi[1],roi[3],step), np.arange(roi[1]+5,roi[3]+5,step)
    duration = datetime.datetime.strptime(end_date, '%Y-%m-%d') - datetime.datetime.strptime(start_date, '%Y-%m-%d')

    for i in range(len(x_start)):
        for j in range(len(y_start)):            
            for k in range(duration.days):
                date = (datetime.datetime.strptime(start_date, '%Y-%m-%d') + datetime.timedelta(k)).strftime('%Y-%m-%d')
                files = glob.glob(os.path.join(root_path+"data/subset/",id,date)+"/**/*.tif")
                for file in files:
                    tasks.append((id, file, [x_start[i], y_start[j], x_stop[i], y_stop[j]], date))
    return tasks


def get_mosaic_tasks(id, start_date,end_date,rois,dir):
    tasks=[]
    duration = datetime.datetime.strptime(end_date, '%Y-%m-%d') - datetime.datetime.strptime(start_date, '%Y-%m-%d')
    for k in range(duration.days):
        for i in range(rois.shape[0]):
            for j in range(rois.shape[1]):
                date = (datetime.datetime.strptime(start_date, '%Y-%m-%d') + datetime.timedelta(k)).strftime('%Y-%m-%d')
                tasks.append((id, rois[i,j,:], date, dir))
    return tasks

def get_json_tasks(target_id, start_date, duration, area_of_interest, products_id, day_night, dir_json, collection_id):
    tasks = []
    for k in range(duration.days):
        tasks.append(
            (
                target_id,
                (datetime.datetime.strptime(start_date, '%Y-%m-%d') + datetime.timedelta(k)).strftime(
                    '%Y-%m-%d'),
                area_of_interest, products_id, day_night, dir_json, collection_id
            )
        )
    return tasks

def get_client_tasks(id, start_date, duration, products_id, day_night, dir_json, dir_nc, collection_id):
    tasks = []
    for k in range(duration.days):
        tasks.append(
            (
                id,
                (datetime.datetime.strptime(start_date, '%Y-%m-%d') + datetime.timedelta(k)).strftime(
                    '%Y-%m-%d'),
                products_id, day_night, dir_json, dir_nc, collection_id
            )
        )
    return tasks

def main_process_wrapper(args):
    return pipeline.processing(*args)

def get_tasks(start_date, duration, id, roi, day_nights, product_id, dir_tifs):
    tasks = []
    for k in range(duration.days):
        for dn in day_nights:
            date = (datetime.datetime.strptime(start_date, '%Y-%m-%d') + datetime.timedelta(k)).strftime('%Y-%m-%d')
            dir_ncs = glob.glob(os.path.join(root_path, "data/VNPNC",id,date,dn,'*'))
            for dir_nc in dir_ncs:
                if not os.listdir(dir_nc):
                    print("Time does not exist:",dir_nc)
                    continue
                time_captured = dir_nc.split('.')[-1][-4:]
                os.makedirs(os.path.join(dir_tifs, id, date, dn), exist_ok=True)
                dir_tif = os.path.join(dir_tifs, id, date, dn, "VNP" + product_id + date +'-'+ time_captured + ".tif")
                if os.path.exists(dir_tif):
                    print("The GEOTIFF for time " + date +'-'+ time_captured + " has been created!")
                    skip_project = True
                else:
                    skip_project = False
                if 'MOD' in product_id:
                    bands = ['M11', 'm_lat', 'm_lon']
                    dir_chan = os.path.join(dir_nc, "M[0-9]*_[0-9]*_[0-9]*.tif")
                else:
                    dir_chan = os.path.join(dir_nc, "I[0-9]*_[0-9]*_[0-9]*.tif")
                    if dn is 'D':
                        bands = ['I01', 'I02', 'I03', 'I04', 'I05', 'i_lat', 'i_lon']
                    else: 
                        bands = ['I04', 'I05', 'i_lat', 'i_lon']
                os.makedirs(os.path.join(root_path, "data/subset/", id, date, dn), exist_ok=True)
                output_path = os.path.join(root_path, "data/subset/", id, date, dn, "VNP" + product_id + date +'-'+ time_captured + ".tif")
                tasks.append(
                    (
                        dir_nc, date, roi, product_id, bands, dir_tif, output_path, dir_chan, skip_project
                    )
                )
    return tasks

def upload_to_gcloud(file, gs_path='gs://ai4wildfire/VNPPROJ5/'):
    print('Upload to gcloud')

    file_name = file.split('/')[-1]
    id = file.split('/')[-2]
    date = file_name[6:16]
    gs_path += id + '/' + file_name
    storage_client = storage.Client()
    bucket = storage_client.bucket('ai4wildfire')
    year = date[:4]
    upload_cmd = 'gsutil cp ' + file + ' '+gs_path
    print(upload_cmd)
    os.system(upload_cmd)
    print('finish uploading' + file_name)

def upload_to_gee(file, gs_path='gs://ai4wildfire/VNPPROJ5/', asset_id='projects/proj5-dataset/assets/proj5_dataset/'):
    print('start uploading to gee')
    file_name = file.split('/')[-1]
    id = file.split('/')[-2]
    date = file_name[6:16]
    time = file.split('/')[-1][17:21]
    gs_path += id + '/' + file_name
    time_start = date + 'T' + time[:2] + ':' + time[2:] + ':00'
    cmd = utils.config.ee_path + ' upload image --force --time_start ' + time_start + ' --asset_id='+asset_id + \
          id+'_'+file_name[:-4] + ' --pyramiding_policy=sample '+gs_path
    print(cmd)
    subprocess.call(cmd.split())
    print('Uploading in progress for image ' + time_start)

def upload_to_gcloud_hdf(file, gs_path='gs://ai4wildfire/VNPPROJ5/'):
    print('Upload to gcloud')

    file_name = file.split('/')[-1]
    id = file.split('/')[-3]
    date = file.split('/')[-2]
    gs_path += id + '/' + date + '/' + file_name
    upload_cmd = 'gsutil cp ' + file + ' '+gs_path
    print(upload_cmd)
    os.system(upload_cmd)
    print('finish uploading' + file_name)

def upload_to_gee_hdf(file, gs_path='gs://ai4wildfire/VNPPROJ5/', asset_id='projects/proj5-dataset/assets/proj5_dataset/'):
    print('start uploading to gee')
    file_name = file.split('/')[-1]
    id = file.split('/')[-3]
    start_date = file.split('/')[-2]

    product_id = file_name.split('.')[0]
    position = file_name.split('.')[2]
    gs_path += id + '/' + start_date + '/' + file_name
    if 'MOD' in product_id:
        time_start = start_date + 'T10:30:00'
    elif 'VNP' in product_id:
        time_start = start_date + 'T13:30:00'
    else:
        raise 'product_id not found'

    cmd = utils.config.ee_path + ' upload image --force --time_start ' + time_start + ' --asset_id='+asset_id + \
          product_id+'_'+id+'_'+position+'_'+start_date + ' --pyramiding_policy=sample '+gs_path
    print(cmd)
    subprocess.call(cmd.split())
    print('Uploading in progress for image ' + time_start)

def upload(file, asset_id):
    upload_to_gcloud(file)
    upload_to_gee(file, asset_id=asset_id)

def upload_hdf(file, asset_id):
    upload_to_gcloud_hdf(file)
    upload_to_gee_hdf(file, asset_id=asset_id)

def download_af_from_firms(url, save_folder):
    print(url)

    save_name = os.path.split(url)[-1]
    dst = Path(save_folder) / save_name
    save_folder = Path(os.path.split(dst)[0])
    unzip_folder = save_folder / "unzipped"

    logging.basicConfig(
        format='%(asctime)s %(levelname)s %(message)s',
        level=logging.INFO,
        stream=sys.stdout)

    if os.path.isfile(dst):
        os.system("rm {}".format(dst))
        logging.info("Existed file deleted: {}".format(dst))
    else:
        logging.info("File doesn't exist.")
    # replace with url you need

    # if dir 'dir_name/' doesn't exist
    if not os.path.exists(save_folder):
        logging.info("Make direction: {}".format(save_folder))
        os.mkdir(save_folder)

    def down(_save_path, _url):
        try:
            Request.urlretrieve(_url, _save_path)
            return True
        except:
            print('\nError when retrieving the URL:\n{}'.format(_url))
            return False

    # logging.info("Downloading file.")
    down(dst, url)
    print("------- Download Finished! ---------\n")

def un_zip(src):
    save_folder = Path(os.path.split(src)[0])
    (save_folder / "unzipped").mkdir(exist_ok=True)
    unzip_folder = save_folder / "unzipped" / os.path.split(src)[-1][:-4]

    """ unzip zip file """
    zip_file = zipfile.ZipFile(src)
    if os.path.isdir(unzip_folder):
        pass
    else:
        os.mkdir(unzip_folder)
    for names in zip_file.namelist():
        zip_file.extract(names, unzip_folder)
        zip_file.close()