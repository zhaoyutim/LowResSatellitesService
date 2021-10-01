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

if __name__=='__main__':
    # print(available_readers())
    date_path = 'data/VNPL1'
    date = '252'
    ladsweb_link = 'https://ladsweb.modaps.eosdis.nasa.gov/archive/allData/5110/VNP02IMG/2020/'
    if not os.path.exists(date_path+'/'+date + '/'+date+'.json'):
        my_headers = {'Authorization': 'Bearer emhhb3l1dGltOmVtaGhiM2wxZEdsdFFHZHRZV2xzTG1OdmJRPT06MTYzMzA3NzUzNDo0ZDNlN2I1NmJlNDc3MGM4N2Q5YmVkNDljMzcyMjUzYmMzMzhhMzMw'}
        response = requests.get(ladsweb_link+date+'.json', headers=my_headers)
        print(response.json())

        if not os.path.exists(date_path+'/'+date):
            os.mkdir(date_path+'/'+date)
        with open(date_path+'/'+date + '/'+date+'.json', 'wb') as outf:
            outf.write(response.content)
            print('New file list for day '+date+' created')
    else:
        print('The file list has already been downloaded')
    f = open(date_path+'/'+date + '/'+date+'.json', )
    file_list = json.load(f)
    print(file_list)
    for file in file_list:
        file_name = file['name']
        file_create_time = file['last-modified']
        print(file_name, file_create_time)
        time_captured = file_name.split('.')[2]
        print(time_captured)
        file_link = ladsweb_link + date + '/' + file_name
        print(file_link)
        shutil.rmtree(date_path+'/'+date + '/' + time_captured)
        os.mkdir(date_path+'/'+date + '/' + time_captured)
        wget_command_vnp02 = "wget " + file_link + " --header \"Authorization: Bearer emhhb3l1dGltOmVtaGhiM2wxZEdsdFFHZHRZV2xzTG1OdmJRPT06MTYzMjkzNjUyNjpmMDhhZTI4YzZmODZhNmY1ZjM4NWYwYjY5ODVjNzMxMDIyNTZiMjFl\" -P " + date_path+'/'+date + '/' + time_captured
        # wget_command_vnp03 = "wget " + file_link.replace('VNP02', 'VNP03') + " --header \"Authorization: Bearer emhhb3l1dGltOmVtaGhiM2wxZEdsdFFHZHRZV2xzTG1OdmJRPT06MTYzMjkzNjUyNjpmMDhhZTI4YzZmODZhNmY1ZjM4NWYwYjY5ODVjNzMxMDIyNTZiMjFl\" -P " + date_path+'/'+date + '/' + time_captured
        os.system(wget_command_vnp02)
        # os.system(wget_command_vnp03)

    # path_to_data = '/Users/zhaoyu/PycharmProjects/LowResSatellitesService/data/VNP02IMG/2510000'
    # files = find_files_and_readers(base_dir=path_to_data, reader='viirs_l1b')
    # scn = Scene(filenames=files)
    # print(scn.available_dataset_names())
    # print(scn.available_composite_names())
    # scn.load(['I01','I02','I03','I04','I05', 'i_lat', 'i_lon'])
    #
    # lon=scn['i_lon'].values
    # lat=scn['i_lat'].values
    # area = create_area_def(area_id="area", projection='WGS84', shape=(lat.shape[1],lat.shape[0]), lon=lon, lat=lat)
    # new_scn = scn.resample(destination=area)
    # # compositor = GenericCompositor("overview")
    # # composite = compositor([new_scn['I01'],new_scn['I02'],new_scn['I03'],new_scn['I04'],new_scn['I05']])
    #
    # new_scn.save_datasets(
    #     filename='{name}_{start_time:%Y%m%d_%H%M%S}.tif',
    #     datasets=['I01','I02','I03','I04','I05'],
    #     base_dir=path_to_data)
    #
    # # list all files in directory that match pattern
    # demList = glob.glob(path_to_data + "/I[0-9]*_[0-9]*_[0-9]*.tif")
    # demList.sort()
    # demList = ' '.join(map(str, demList))
    # print(demList)
    #
    # # gdal_merge
    # cmd = "gdalbuildvrt -srcnodata 0 -vrtnodata 0 -separate stack.vrt " + demList
    # subprocess.call(cmd.split())
    #
    # cmd = "gdal_translate stack.vrt stack.tif"
    # subprocess.call(cmd.split())
