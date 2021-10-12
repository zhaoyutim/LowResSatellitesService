import os
import shutil
import datetime
from pprint import pprint

from satpy.scene import Scene
from satpy import find_files_and_readers
from pyresample import create_area_def
import numpy as np
import glob
import subprocess
import requests
import json
import argparse
class Pipeline:
    def __init__(self):
        return

    def read_and_projection(self, date, dir_data='../data/VNPL1'):
        dir_list = glob.glob(dir_data+'/'+date+'/*/')
        for dir_nc in dir_list:
            # if len(glob.glob(dir_nc+'/*.nc'))!=2:
            #     print('Current download not complete')
            #     continue
            time_captured = dir_nc.split('/')[-2]
            print(time_captured)
            if os.path.exists('../data/VNPIMGTIF/' + date + '/' +time_captured+ "/VNPIMG" + date +'-'+ time_captured + ".tif"):
                print("The GEOTIFF for time " + date +'-'+ time_captured + " has been created!")
                continue
            #TODO find a way to divide day night images
            if int(time_captured)>1200:
                list_of_bands = ['I01', 'I02', 'I03', 'I04', 'I05', 'i_lat', 'i_lon']
            else:
                list_of_bands = ['I04', 'I05', 'i_lat', 'i_lon']
            try:
                files = find_files_and_readers(base_dir=dir_nc, reader='viirs_l1b')
                scn = Scene(filenames=files)
                scn.load(list_of_bands)

                lon = scn['i_lon'].values
                lat = scn['i_lat'].values
                area = create_area_def(area_id="area", projection='WGS84', shape=(lat.shape[1], lat.shape[0]), lon=lon, lat=lat)
                new_scn = scn.resample(destination=area)

                # scene_llbox = new_scn.crop(xy_bbox=roi)

                new_scn.save_datasets(
                    writer='geotiff', dtype=np.float32, enhance=False,
                    filename='{name}_{start_time:%Y%m%d_%H%M%S}.tif',
                    datasets=list_of_bands[:-2],
                    base_dir=dir_nc)

                demList = glob.glob(dir_nc + "/I[0-9]*_[0-9]*_[0-9]*.tif")
                demList.sort()
                demList = ' '.join(map(str, demList))
                print(demList)

                # gdal_merge
                cmd = "gdalbuildvrt -srcnodata 0 -vrtnodata 0 -separate " + dir_nc + "/VNPIMG" + \
                      date +'-'+ time_captured + ".vrt " + demList
                subprocess.call(cmd.split())

                cmd = "gdal_translate " + dir_nc + "/VNPIMG" + \
                      date +'-'+ time_captured + ".vrt " + dir_nc.replace('VNPL1', 'VNPIMGTIF') + "/VNPIMG" + \
                      date +'-'+ time_captured + ".tif"
                subprocess.call(cmd.split())
            except:
                print('Fail to generate GEOTIFF for '+date+'-'+time_captured)
                pass

    def upload_to_gcloud(self, date, dir_tif='../data/VNPIMGTIF'):
        file_list = glob.glob(dir_tif+'/'+date+'/*/*.tif')
        file_list.sort()
        if not os.path.exists('../data/cogtif'):
            os.mkdir('../data/cogtif')
        for file in file_list:
            time = file.split('/')[-2]
            file_name = file.split('/')[-1]
            print(date+'/'+time+'/'+file_name)
            if not os.path.exists('../data/cogtif/'+date):
                os.mkdir('../data/cogtif/'+date)
            print('Cloud Optimization')
            os.system('rio cogeo create '+ file +' ../data/cogtif/'+date+'/'+file_name)
            print('Upload to gcloud')
            upload_cmd = 'gsutil cp ../data/cogtif/'+date+'/'+file_name + ' gs://ai4wildfire/' + 'VNPIMGTIF/2021' + date + '/' + file_name
            os.system(upload_cmd)
            print('finish uploading'+file)

    def get_time_start(self, date_string):
        year = date_string[:4]
        day = datetime.date.fromisoformat(year + '-01-01') + datetime.timedelta(days=int(date_string[4:7]) - 1)
        hour = date_string[7:9]
        min = date_string[9:]
        return str(day) + 'T' + hour + ':' + min + ':' + '00'

    def upload_to_gee(self, dir_tif='../data/cogtif'):
        file_list = glob.glob('dir_tif+/*/*/*.tif')
        for file in file_list:
            date = file.split('/')[-2]
            file_name = file.split('/')[-1]
            time = file_name[-8:-4]
            time_start = self.get_time_start(file_name[7:-4])
            if int(time) <= 1200:
                cmd = 'earthengine upload image --time_start ' + time_start + ' --asset_id=projects/grand-drive-285514/assets/viirs_night/' + \
                      file.split('\\')[-1][
                      :-4] + ' --pyramiding_policy=sample gs://ai4wildfire/' + 'VNPIMGTIF/2021' + date + '/' + file_name
                subprocess.call(cmd.split())
            else:
                cmd = 'earthengine upload image --time_start ' + time_start + ' --asset_id=projects/grand-drive-285514/assets/viirs_day/' + \
                      file.split('\\')[-1][
                      :-4] + ' --pyramiding_policy=sample gs://ai4wildfire/' + 'VNPIMGTIF/2021' + date + '/' + file_name
                subprocess.call(cmd.split())

    def processing(self, date, dir_data='../data/VNPL1'):
        print("Start Projection Locally")
        # self.read_and_projection(date)
        print("Start uploading to the cloud")
        self.upload_to_gcloud(date, dir_data.replace('VNPL1', 'VNPIMGTIF'))
        self.upload_to_gee()



if __name__ == '__main__':
    pipeline = Pipeline()
    pipeline.processing('2021-07-14')