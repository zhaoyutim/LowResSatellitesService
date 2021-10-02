import os
import subprocess
from glob import glob
from google.cloud import storage


def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    # The ID of your GCS bucket
    # bucket_name = "your-bucket-name"
    # The path to your file to upload
    # source_file_name = "local/path/to/file"
    # The ID of your GCS object
    # destination_blob_name = "storage-object-name"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    print(
        "File {} uploaded to {}.".format(
            source_file_name, destination_blob_name
        )
    )

if __name__ == '__main__':
    file_dir = '/Users/zhaoyu/PycharmProjects/LowResSatellitesService/data/VNPIMGTIF/253'
    file_list = glob(file_dir+'/*/*.tif')
    file_list.sort()
    # for file in file_list:
    #     print(file.split('/')[-3]+'/'+file.split('/')[-2]+'/'+file.split('/')[-1])
    #     os.system('rio cogeo create '+ file +' data/cogtif/'+file.split('/')[-1])
    file_list = glob('/Users/zhaoyu/PycharmProjects/LowResSatellitesService/data/cogtif/*.tif')
    file_list.sort()
    # for file in file_list:
    #     upload_cmd = 'gsutil cp '+file+' gs://ai4wildfire/'+'VNPIMGTIF/2020253/'+file.split('/')[-1]
    #     subprocess.call(upload_cmd.split())
    for file in file_list:
        # upload_blob('ai4wildfire', file, 'VNPIMGTIF'+file.split('/')[-3]+'/'+file.split('/')[-2]+'/'+file.split('/')[-1])
        cmd = 'earthengine upload image --asset_id=projects/grand-drive-285514/assets/viirs_375/'+file.split('/')[-1][:-4]+' --pyramiding_policy=sample gs://ai4wildfire/'+'VNPIMGTIF/2020253/'+file.split('/')[-1]
        subprocess.call(cmd.split())
        # cmd_set_property = 'earthengine asset set --time_start {time_start} {asset_id}'
        # subprocess.call(cmd_set_property.split())
        # print(file.split('/')[-1])
