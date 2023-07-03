import glob
import multiprocessing
import os
import subprocess

from google.cloud import storage


def upload_to_gcloud(file):
    print('Upload to gcloud')
    file_name = file.split('/')[-1]
    id = file.split('/')[-2]
    date = file_name[6:16]
    storage_client = storage.Client()
    bucket = storage_client.bucket('ai4wildfire')
    year = date[:4]
    upload_cmd = 'gsutil cp ' + file + ' gs://ai4wildfire/VNPPROJ5/'+id+'/' + file_name
    print(upload_cmd)
    os.system(upload_cmd)
    print('finish uploading' + file_name)


def upload_to_gee(file, day_night):
    print('start uploading to gee')
    file_name = file.split('/')[-1]
    id = file.split('/')[-2]
    date = file_name[6:16]
    time = file.split('/')[-1][17:21]
    time_start = date + 'T' + time[:2] + ':' + time[2:] + ':00'
    if day_night=='D':
        cmd = 'earthengine upload image --time_start ' + time_start + ' --asset_id=projects/proj5-dataset/assets/proj5_dataset/' + \
              id+'_'+file_name[:-4] + ' --pyramiding_policy=sample gs://ai4wildfire/VNPPROJ5/'+id+'/' + file_name
    else:
        cmd = 'earthengine upload image --time_start ' + time_start + ' --asset_id=projects/proj5-dataset/assets/proj5_dataset_night/' + \
              id + '_' + file_name[
                         :-4] + ' --pyramiding_policy=sample gs://ai4wildfire/VNPPROJ5/' + id + '/' + file_name
    print(cmd)
    subprocess.call(cmd.split())
    print('Uploading in progress for image ' + time_start)

def upload(file, day_night):
    upload_to_gcloud(file)
    upload_to_gee(file, day_night)

def upload_in_parallel(import_all=True, day_night='D', filepath='data/subset'):
    if import_all:
        file_list = glob.glob(os.path.join(filepath, '*.tif'))
    else:
        log_path = 'log/sanity_check_gee*.log'
        log_list = glob.glob(log_path)
        log_list.sort()
        with open(log_list[-1]) as f:
            f = f.readlines()
        file_list = []
        for line in f:
            file_list.append(os.path.join(filepath, line.split('_')[1],line.split('_')[2].replace('\n', '')+'.tif'))

    results = []
    with multiprocessing.Pool(processes=8) as pool:
        for file in file_list:
            id = file.split('/')[-2]
            date = file.split('/')[-1][6:16]
            time = file.split('/')[-1][17:21]
            vnp_json = open(glob.glob(os.path.join('data/VNPL1', id, date, day_night, '*.json'))[0], 'rb')
            import json
            def get_name(json):
                return json.get('name').split('.')[2]
            vnp_time = list(map(get_name, json.load(vnp_json)['content']))
            if time not in vnp_time or 'IMG' not in file:
                continue
            result = pool.apply_async(upload, (file, day_night))
            results.append(result)
        results = [result.get() for result in results if result is not None]

def upload_by_log(filepath='data/subset'):
    with open('log/error', 'r') as f:
        file = f.read().split('\n')

    def get_id(dir_str):
        return dir_str.split('/')[1]

    target_ids = list(map(get_id, file))

    def get_date(dir_str):
        return dir_str.split('/')[-1][:10]

    target_dates = list(map(get_date, file))
    for i, target_id in enumerate(target_ids):
        tif_list = glob.glob(os.path.join(filepath, target_id, 'VNPIMG' + target_dates[i] + '*.tif'))
        for tif_file in tif_list:
            os.system('geeadd delete --id '+'projects/proj5-dataset/assets/proj5_dataset_night/'+target_id+'_'+tif_file.split('/')[-1][:-4])
            upload(tif_file)
if __name__=='__main__':
    ids = ['alberta_fire_3', 'alberta_fire_4',
          'alberta_fire_5', 'alberta_fire_6', 'alberta_fire_7', 'alberta_fire_8', 'alberta_fire_9',
          'alberta_fire_10', 'alberta_fire_11', 'alberta_fire_12', 'quebec_fire_0', 'quebec_fire_1',
          'quebec_fire_2', 'quebec_fire_3']
    for id in ids:
        print(id)
        upload_in_parallel(True, 'D', 'data/subset/'+id)
    # upload_by_log()
