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

logger = logging.getLogger(__name__)

laads_client = LaadsClient()
pipeline = Pipeline()

def json_wrapper(args):
    return laads_client.query_filelist_with_date_range_and_area_of_interest(*args)


def client_wrapper(args):
    return laads_client.download_files_to_local_based_on_filelist(*args)


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

def get_tasks(start_date, duration, id, roi, day_nights, utmzone, product_id, dir_nc, dir_tif, dir_subset):
    tasks = []
    for k in range(duration.days):
        tasks.append(
            (
                (datetime.datetime.strptime(start_date, '%Y-%m-%d') + datetime.timedelta(k)).strftime('%Y-%m-%d'),
                id, roi, day_nights, utmzone, product_id, dir_nc, dir_tif, dir_subset
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
    cmd = '/geoinfo_vol1/home/z/h/zhao2/mambaforge/envs/rioxarray_env/bin/earthengine upload image --force --time_start ' + time_start + ' --asset_id='+asset_id + \
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

    cmd = '/geoinfo_vol1/home/z/h/zhao2/mambaforge/envs/rioxarray_env/bin/earthengine upload image --force --time_start ' + time_start + ' --asset_id='+asset_id + \
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