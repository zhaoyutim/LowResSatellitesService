import os
import shutil

import wget as wget
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
    parser.add_argument('date', type=str,
                        help='assign processing date')
    args = parser.parse_args()

    date_path = 'data/VNPL1'
    save_path = 'data/VNPIMGTIF'
    date = args.date
    ladsweb_link_vnp02 = 'https://ladsweb.modaps.eosdis.nasa.gov/archive/allData/5110/VNP02IMG/2020/'
    ladsweb_link_vnp03 = 'https://ladsweb.modaps.eosdis.nasa.gov/archive/allData/5110/VNP03IMG/2020/'
    if not os.path.exists(date_path+'/'+date + '/'+date+'.json'):
        my_headers = {'Authorization': 'Bearer emhhb3l1dGltOmVtaGhiM2wxZEdsdFFHZHRZV2xzTG1OdmJRPT06MTYzMzA3NzUzNDo0ZDNlN2I1NmJlNDc3MGM4N2Q5YmVkNDljMzcyMjUzYmMzMzhhMzMw'}
        response_vnp02 = requests.get(ladsweb_link_vnp02+date+'.json', headers=my_headers)
        response_vnp03 = requests.get(ladsweb_link_vnp03 + date + '.json', headers=my_headers)

        if not os.path.exists(date_path+'/'+date):
            os.mkdir(date_path+'/'+date)
        if not os.path.exists(save_path + '/' + date):
            os.mkdir(save_path+'/'+date)
        with open(date_path+'/'+date + '/'+date+'_vnp02.json', 'wb') as outf:
            outf.write(response_vnp02.content)
        with open(date_path + '/' + date + '/' + date + '_vnp03.json', 'wb') as outf:
            outf.write(response_vnp03.content)
        print('New file list for day '+date+' created')

    else:
        print('The file list has already been downloaded')
    vnp02_json = open(date_path+'/'+date + '/'+date+'_vnp02.json', )
    vnp02_list = json.load(vnp02_json)

    vnp03_json = open(date_path+'/'+date + '/'+date+'_vnp03.json', )
    vnp03_list = json.load(vnp03_json)
    for (vnp02_file, vnp03_file) in zip(vnp02_list, vnp03_list):


        vnp02_name = vnp02_file['name']
        vnp03_name = vnp03_file['name']
        vnp02_create_time = vnp02_file['last-modified']

        time_captured = vnp02_name.split('.')[2]
        path_to_data = date_path + '/' + date + '/' + time_captured
        path_to_geotiff = save_path + '/' + date + '/' + time_captured
        if os.path.exists(path_to_geotiff + "/VNPIMG" + vnp02_name.split('.')[1] + vnp02_name.split('.')[2] + ".tif"):
            print("The GEOTIFF for time "+vnp02_name.split('.')[1] + vnp02_name.split('.')[2]+" has been created!")
            continue
        print(time_captured)
        vnp02_link = ladsweb_link_vnp02 + date + '/' + vnp02_name
        vnp03_link = ladsweb_link_vnp03 + date + '/' + vnp03_name
        # Keep a clean directory before downloading
        if not os.path.exists(date_path + '/' + date + '/' + time_captured):
            os.mkdir(date_path + '/' + date + '/' + time_captured)
        if not os.path.exists(save_path + '/' + date + '/' + time_captured):
            os.mkdir(save_path + '/' + date + '/' + time_captured)
        if not os.path.exists(date_path+'/'+date + '/' + time_captured+'/'+vnp02_name) or not os.path.exists(date_path+'/'+date + '/' + time_captured+'/'+vnp03_name):
            print("Downloading netCDF files " + vnp02_name.split('.')[1] + vnp02_name.split('.')[2] + " from Remote server")
            shutil.rmtree(date_path+'/'+date + '/' + time_captured)
            wget_command_vnp02 = "wget " + vnp02_link + " --header \"Authorization: Bearer emhhb3l1dGltOmVtaGhiM2wxZEdsdFFHZHRZV2xzTG1OdmJRPT06MTYzMjkzNjUyNjpmMDhhZTI4YzZmODZhNmY1ZjM4NWYwYjY5ODVjNzMxMDIyNTZiMjFl\" -P " + date_path+'/'+date + '/' + time_captured
            wget_command_vnp03 = "wget " + vnp03_link + " --header \"Authorization: Bearer emhhb3l1dGltOmVtaGhiM2wxZEdsdFFHZHRZV2xzTG1OdmJRPT06MTYzMjkzNjUyNjpmMDhhZTI4YzZmODZhNmY1ZjM4NWYwYjY5ODVjNzMxMDIyNTZiMjFl\" -P " + date_path+'/'+date + '/' + time_captured
            os.system(wget_command_vnp02)
            os.system(wget_command_vnp03)


        files = find_files_and_readers(base_dir=path_to_data, reader='viirs_l1b')
        scn = Scene(filenames=files)
        scn.load(['I01','I02','I03','I04','I05', 'i_lat', 'i_lon'])

        lon=scn['i_lon'].values
        lat=scn['i_lat'].values
        area = create_area_def(area_id="area", projection='WGS84', shape=(lat.shape[1],lat.shape[0]), lon=lon, lat=lat)
        new_scn = scn.resample(destination=area)
        # compositor = GenericCompositor("overview")
        # composite = compositor([new_scn['I01'],new_scn['I02'],new_scn['I03'],new_scn['I04'],new_scn['I05']])

        new_scn.save_datasets(
            filename='{name}_{start_time:%Y%m%d_%H%M%S}.tif',
            datasets=['I01','I02','I03','I04','I05'],
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
