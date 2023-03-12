import glob
import multiprocessing
import os
import subprocess

from google.cloud import storage


def upload_to_gcloud(file):
    print('Upload to gcloud')
    file_name = file.split('\\')[-1]
    id = file.split('\\')[-2]
    date = file_name[6:16]
    storage_client = storage.Client()
    bucket = storage_client.bucket('ai4wildfire')
    year = date[:4]
    upload_cmd = 'gsutil cp ' + file + ' gs://ai4wildfire/VNPPROJ5/'+id+'/' + file_name
    print(upload_cmd)
    os.system(upload_cmd)
    print('finish uploading' + file_name)


def upload_to_gee(file):
    print('start uploading to gee')
    file_name = file.split('\\')[-1]
    id = file.split('\\')[-2]
    date = file_name[6:16]
    time = file.split('\\')[-1][17:21]
    time_start = date + 'T' + time[:2] + ':' + time[2:] + ':00'
    cmd = 'earthengine upload image --time_start ' + time_start + ' --asset_id=projects/proj5-dataset/assets/proj5_dataset/' + \
          id+'_'+file_name[:-4] + ' --pyramiding_policy=sample gs://ai4wildfire/VNPPROJ5/'+id+'/' + file_name
    print(cmd)
    subprocess.call(cmd.split())
    print('Uploading in progress for image ' + time_start)

def upload(file):
    upload_to_gcloud(file)
    upload_to_gee(file)

if __name__=='__main__':
    filepath='E:\\viirs\\subset'
    file_list = glob.glob(os.path.join(filepath, '*', '*.tif'))
    results = []
    with multiprocessing.Pool(processes=8) as pool:
        for file in file_list:
            result = pool.apply_async(upload, (file,))
            results.append(result)
        results = [result.get() for result in results if result is not None]