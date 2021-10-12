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

if __name__=='__main__':
    parser = argparse.ArgumentParser(description='assign processing date')
    parser.add_argument('--date', '-d', type=str, help='assign processing date')
    args = parser.parse_args()

    date_path = 'data/VNPL1'
    save_path = 'data/VNPIMGTIF'
    roi=(-124.48, 32.54, -114.06, 41.98)

    date_ndays = args.date
    year = '2021'
    date = datetime.datetime.strptime(year + '-01-01', '%Y-%m-%d') + datetime.timedelta(int(date_ndays)-1)

    if not os.path.exists(date_path+'/'+date.strftime('%Y-%m-%d') + '/'+date.strftime('%Y-%m-%d')+'.json'):
        # Query server with roi
        ladsweb_link_vnp02 = 'https://ladsweb.modaps.eosdis.nasa.gov/api/v1/files/product=VNP02IMG&collection=5200&dateRanges=' + date.strftime(
            '%Y-%m-%d') + '&areaOfInterest=x-129.5y56.2,x-110.4y31.7'
        ladsweb_link_vnp03 = 'https://ladsweb.modaps.eosdis.nasa.gov/api/v1/files/product=VNP03IMG&collection=5200&dateRanges=' + date.strftime(
            '%Y-%m-%d') + '&areaOfInterest=x-129.5y56.2,x-110.4y31.7'

        my_headers = {
            "X-Requested-With": "XMLHttpRequest",
            'Authorization': 'Bearer emhhb3l1dGltOmVtaGhiM2wxZEdsdFFHZHRZV2xzTG1OdmJRPT06MTYzMzk0NzU0NTphZmRlYWY2MjE2ODg0MjQ5MTEzNmE3MTE4MzZkOWYxYjg3MWQzNWMz'}

        response_vnp02 = requests.get(ladsweb_link_vnp02, headers=my_headers)
        response_vnp03 = requests.get(ladsweb_link_vnp03, headers=my_headers)
        date = date.strftime('%Y-%m-%d')
        if not os.path.exists(date_path + '/' + date):
            os.mkdir(date_path + '/' + date)
        if not os.path.exists(save_path + '/' + date):
            os.mkdir(save_path + '/' + date)
        with open(date_path + '/' + date + '/' + date + '_vnp02.json', 'wb') as outf:
            outf.write(response_vnp02.content)
        with open(date_path + '/' + date + '/' + date + '_vnp03.json', 'wb') as outf:
            outf.write(response_vnp03.content)
        print('New file list for day '+date+' created')

    else:
        print('The file list has already been downloaded')
    vnp02_json = open(date_path+'/'+date + '/'+date+'_vnp02.json', )
    vnp02_list = json.load(vnp02_json)
    pprint(vnp02_list)

    vnp03_json = open(date_path+'/'+date + '/'+date+'_vnp03.json', )
    vnp03_list = json.load(vnp03_json)
    download_link_vnp02 = 'https://ladsweb.modaps.eosdis.nasa.gov/archive/allData/5200/VNP02IMG/'+year+'/'
    download_link_vnp03 = 'https://ladsweb.modaps.eosdis.nasa.gov/archive/allData/5200/VNP03IMG/'+year+'/'
    print('There are '+ str(vnp03_list.__len__()) +' files in total.')
    for (vnp02_file, vnp03_file) in zip(vnp02_list, vnp03_list):
        vnp02_file = vnp02_list[vnp02_file]
        vnp03_file = vnp03_list[vnp03_file]

        vnp02_name = vnp02_file['name']
        vnp03_name = vnp03_file['name']
        time_captured = vnp02_name.split('.')[2]
        path_to_data = date_path + '/' + date + '/' + time_captured
        path_to_geotiff = save_path + '/' + date + '/' + time_captured

        # if not (int(time_captured)>=1900 and int(time_captured)<=2100 or int(time_captured)>=800 and int(time_captured)<=1000):
        #     continue

        if os.path.exists(path_to_geotiff + "/VNPIMG" + vnp02_name.split('.')[1] + vnp02_name.split('.')[2] + ".tif"):
            print("The GEOTIFF for time "+vnp02_name.split('.')[1] + vnp02_name.split('.')[2]+" has been created!")
            continue

        print(time_captured)
        vnp02_link = download_link_vnp02 + date_ndays + '/' + vnp02_name
        vnp03_link = download_link_vnp03 + date_ndays + '/' + vnp03_name
        # Keep a clean directory before downloading
        if not os.path.exists(date_path + '/' + date + '/' + time_captured):
            os.mkdir(date_path + '/' + date + '/' + time_captured)
        if not os.path.exists(save_path + '/' + date + '/' + time_captured):
            os.mkdir(save_path + '/' + date + '/' + time_captured)
        if not os.path.exists(date_path+'/'+date + '/' + time_captured+'/'+vnp02_name) or not os.path.exists(date_path+'/'+date + '/' + time_captured+'/'+vnp03_name):
            print("Downloading netCDF files " + vnp02_name.split('.')[1] + vnp02_name.split('.')[2] + " from Remote server")
            shutil.rmtree(date_path+'/'+date + '/' + time_captured)
            wget_command_vnp02 = "wget " + vnp02_link + " --header \"Authorization: Bearer emhhb3l1dGltOmVtaGhiM2wxZEdsdFFHZHRZV2xzTG1OdmJRPT06MTYzMzk2ODc2ODpmYjFmNGVkNzNkNWI0NGU3MGE1YzMyODExYmNhNGU0Njg3NzBjMGZj\" -P " + date_path+'/'+date + '/' + time_captured
            wget_command_vnp03 = "wget " + vnp03_link + " --header \"Authorization: Bearer emhhb3l1dGltOmVtaGhiM2wxZEdsdFFHZHRZV2xzTG1OdmJRPT06MTYzMzk2ODc2ODpmYjFmNGVkNzNkNWI0NGU3MGE1YzMyODExYmNhNGU0Njg3NzBjMGZj\" -P " + date_path+'/'+date + '/' + time_captured
            os.system(wget_command_vnp02)
            os.system(wget_command_vnp03)

        try:
            files = find_files_and_readers(base_dir=path_to_data, reader='viirs_l1b')
            scn = Scene(filenames=files)
            scn.load(['I01','I02','I03','I04','I05', 'i_lat', 'i_lon'])

            lon=scn['i_lon'].values
            lat=scn['i_lat'].values
            area = create_area_def(area_id="area", projection='WGS84', shape=(lat.shape[1],lat.shape[0]), lon=lon, lat=lat)
            new_scn = scn.resample(destination=area)
            # compositor = GenericCompositor("overview")
            # composite = compositor([new_scn['I01'],new_scn['I02'],new_scn['I03'],new_scn['I04'],new_scn['I05']])
            # scene_llbox = new_scn.crop(xy_bbox=roi)

            new_scn.save_datasets(
                writer='geotiff', dtype=np.float32, enhance=False,
                filename='{name}_{start_time:%Y%m%d_%H%M%S}.tif',
                datasets=['I01', 'I02', 'I03', 'I04', 'I05'],
                base_dir=path_to_data)

            # list all files in directory that match pattern
            demList = glob.glob(path_to_data + "/I[0-9]*_[0-9]*_[0-9]*.tif")
            demList.sort()
            demList = ' '.join(map(str, demList))
            print(demList)

            # gdal_merge
            cmd = "gdalbuildvrt -srcnodata 0 -vrtnodata 0 -separate " + path_to_data + "/VNPIMG"+vnp02_name.split('.')[1]+vnp02_name.split('.')[2]+".vrt " + demList
            subprocess.call(cmd.split())

            cmd = "gdal_translate " + path_to_data + "/VNPIMG"+vnp02_name.split('.')[1]+vnp02_name.split('.')[2]+".vrt " + path_to_geotiff +"/VNPIMG"+vnp02_name.split('.')[1]+vnp02_name.split('.')[2]+".tif"
            subprocess.call(cmd.split())

        except:
            try:
                files = find_files_and_readers(base_dir=path_to_data, reader='viirs_l1b')
                scn = Scene(filenames=files)
                scn.load(['I04', 'I05', 'i_lat', 'i_lon'])

                lon = scn['i_lon'].values
                lat = scn['i_lat'].values
                area = create_area_def(area_id="area", projection='WGS84', shape=(lat.shape[1], lat.shape[0]), lon=lon,
                                       lat=lat)
                new_scn = scn.resample(destination=area)
                # compositor = GenericCompositor("overview")
                # composite = compositor([new_scn['I01'],new_scn['I02'],new_scn['I03'],new_scn['I04'],new_scn['I05']])

                # scene_llbox = new_scn.crop(xy_bbox=roi)

                new_scn.save_datasets(
                    writer='geotiff', dtype=np.float32, enhance=False,
                    filename='{name}_{start_time:%Y%m%d_%H%M%S}.tif',
                    datasets=['I04', 'I05'],
                    base_dir=path_to_data)

                # list all files in directory that match pattern
                demList = glob.glob(path_to_data + "/I[0-9]*_[0-9]*_[0-9]*.tif")
                demList.sort()
                demList = ' '.join(map(str, demList))
                print(demList)

                # gdal_merge
                cmd = "gdalbuildvrt -srcnodata 0 -vrtnodata 0 -separate " + path_to_data + "/VNPIMG" + \
                      vnp02_name.split('.')[1] + vnp02_name.split('.')[2] + ".vrt " + demList
                subprocess.call(cmd.split())

                cmd = "gdal_translate " + path_to_data + "/VNPIMG" + vnp02_name.split('.')[1] + vnp02_name.split('.')[
                    2] + ".vrt " + path_to_geotiff + "/VNPIMG" + vnp02_name.split('.')[1] + vnp02_name.split('.')[
                          2] + ".tif"
                subprocess.call(cmd.split())

            except:
                print('fail to create image')
                pass
 