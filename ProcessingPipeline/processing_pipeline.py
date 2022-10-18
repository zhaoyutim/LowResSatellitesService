import glob
import os
import subprocess

import numpy as np
from google.cloud import storage
from pyresample import create_area_def
from satpy import find_files_and_readers
from satpy.scene import Scene


class Pipeline:
    def __init__(self):
        return

    def read_and_projection(self, date, roi, dir_data='data/VNPL1'):
        dir_list = glob.glob(dir_data+'/'+date+'/*/')
        dir_list.sort()
        for dir_nc in dir_list:
            dir_nc = dir_nc.replace('\\', '/')
            if len(glob.glob(dir_nc+'/*.nc'))!=2:
                print('Current download not complete')
                continue
            time_captured = dir_nc.split('/')[-2]
            if 'MOD' in dir_data:
                save_path = dir_data.replace('VNPL1', 'VNPMODTIF')
                # TODO: This part need reimplementation, the query for day/night bands are in place but the directory is not ready.
                if int(time_captured) > 1200:
                    # list_of_bands = ['I01', 'I02', 'I03', 'I04', 'I05', 'i_lat', 'i_lon']
                    list_of_bands = ['M11', 'm_lat', 'm_lon']
                else:
                    # list_of_bands = ['I04', 'I05', 'i_lat', 'i_lon']
                    list_of_bands = ['M11', 'm_lat', 'm_lon']
            else:
                save_path = dir_data.replace('VNPL1', 'VNPIMGTIF')
                if int(time_captured) > 1200:
                    list_of_bands = ['I01', 'I02', 'I03', 'I04', 'I05', 'i_lat', 'i_lon']
                else:
                    list_of_bands = ['I04', 'I05', 'i_lat', 'i_lon']
            if not os.path.exists(save_path):
                os.mkdir(save_path)
            if not os.path.exists(save_path + '/' + date):
                os.mkdir(save_path + '/' + date)
            print(time_captured)
            if os.path.exists('data/VNPIMGTIF/' + date + '/' +time_captured+ "/VNPIMG" + date +'-'+ time_captured + ".tif"):
                print("The GEOTIFF for time " + date +'-'+ time_captured + " has been created!")
                continue
            files = find_files_and_readers(base_dir=dir_nc, reader='viirs_l1b')
            scn = Scene(filenames=files)
            print(scn.available_dataset_ids())
            scn.load(list_of_bands)
            if 'MOD' in dir_data:
                lon = scn['m_lon'].values
                lat = scn['m_lat'].values
            else:
                lon = scn['i_lon'].values
                lat = scn['i_lat'].values
            area = create_area_def(area_id="area", projection='WGS84', shape=(lat.shape[1], lat.shape[0]), lon=lon, lat=lat)
            new_scn = scn.resample(destination=area)

            # scene_llbox = new_scn.crop(xy_bbox=roi)

            if not os.path.exists(save_path + '/' + date + '/' + time_captured):
                os.mkdir(save_path + '/' + date + '/' + time_captured)
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

    def cloud_optimization(self, date, file):
        file_name = file.split('/')[-1]
        # if not os.path.exists('data/cogtif'):
        #     os.mkdir('data/cogtif')
        # if not os.path.exists('data/cogtif/' + date):
        #     os.mkdir('data/cogtif/' + date)
        print('Cloud Optimization')
        # if not os.path.exists('data/cogtif/' + date + '/' + file_name):
        os.system('rio cogeo create ' + file + ' data/VNPIMGTIF/' + date + '/' + file.split('/')[-2] + '/' + file_name)
        print('Cloud Optimization Finish')
        # else:
        #     print('Cloud Optimized Geotiff already exists')

    def upload_to_gcloud(self, date, file):
        print('Upload to gcloud')
        file_name = file.split('/')[-1]
        storage_client = storage.Client()
        year = date[:4]
        # if not storage.Blob(bucket=bucket, name='VNPIMGTIF/'+year + date + '/' + file_name).exists(storage_client):
        upload_cmd = 'gsutil cp ' + file + ' gs://ai4wildfire/' + 'VNPIMGTIF/'+year + date + '/' + file_name
        print(upload_cmd)
        os.system(upload_cmd)
        print('finish uploading' + file)
        # else:
        #     print('file exist already')

    def upload_to_gee(self, date, file, asset_name='proj3_test_night'):
        print('start uploading to gee')
        time = file.split('/')[-1][-8:-4]
        year = date[:4]
        file_name = file.split('/')[-1]
        time_start = date + 'T' + time[:2] + ':' + time[2:] + ':00'
        if int(time) <= 1200:
            cmd = 'earthengine upload image --time_start ' + time_start + ' --asset_id=projects/ee-zhaoyutim/assets/swedish_fire_night/' + \
                  file.split('/')[-1][
                  :-4] + ' --pyramiding_policy=sample gs://ai4wildfire/' + 'VNPIMGTIF/'+year + date + '/' + file_name
            subprocess.call(cmd.split())
        else:
            cmd = 'earthengine upload image --time_start ' + time_start + ' --asset_id=projects/ee-zhaoyutim/assets/swedish_fire_day/' + \
                  file.split('/')[-1][
                  :-4] + ' --pyramiding_policy=sample gs://ai4wildfire/' + 'VNPIMGTIF/'+year + date + '/' + file_name
            subprocess.call(cmd.split())
        print('Uploading in progress for image '+time_start)

    def crop_to_roi(self, date, roi, file, utmzone):
        if not os.path.exists('data/cogsubset'):
            os.mkdir('data/cogsubset')
        if not os.path.exists('data/cogsubset/'+date):
            os.mkdir('data/cogsubset/'+date)
        cmd='gdalwarp '+'-te ' + str(roi[0]) + ' ' + str(roi[1]) + ' ' + str(roi[2]) + ' ' + str(roi[3]) + ' ' + file + ' ' + os.path.join('data/cogsubset/', date, file.split('/')[-1])

        print(cmd)
        os.system(cmd)
        os.system('gdalwarp -t_srs EPSG:'+utmzone+' -tr 375 375'+file + ' ' + os.path.join('data/cogsubset/', date, file.split('/')[-1]) +' '+os.path.join('data/cogsubset/', date, file.split('/')[-1].replace('VNPIMG', 'VNPIMGPRO')))
        os.remove(os.path.join('data/cogsubset/', date, file.split('/')[-1]))
        # if os.path.getsize(file.replace('cogtif','cogsubset')) <= 50*1000*1000:
        #     os.remove(file.replace('cogtif','cogsubset'))
        #     print('blank image smaller than 50mb, delete')

    def processing(self, date, roi, utmzone, dir_data='data/VNPL1', dir_tif='data/VNPIMGTIF'):
        self.read_and_projection(date, roi, dir_data)
        file_list = glob.glob(dir_tif + '/' + date + '/*/*.tif')
        file_list.sort()
        for file in file_list:
            # file = file.replace('\\', '/')
            self.cloud_optimization(date, file)
            self.crop_to_roi(date, roi, dir_tif +'/'+ date + '/'+file.split('/')[-2]+'/'+file.split('/')[-1], utmzone)
            self.upload_to_gcloud(date, 'data/cogsubset/'+date+'/'+file.split('/')[-1].replace('VNPIMG', 'VNPIMGPRO'))
        file_list = glob.glob(dir_tif.replace('VNPIMGTIF', 'cogsubset') + '/' + date + '/*.tif')
        file_list.sort()
        for file in file_list:
            # file = file.replace('\\', '/')
            self.upload_to_gee(date, file)



if __name__ == '__main__':
    pipeline = Pipeline()
    pipeline.processing('2020-08-09')