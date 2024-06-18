import glob
import logging
import os
import time
from logging.handlers import RotatingFileHandler
import ee

from main_mosaic import read_tiff
import numpy as np

def sanity_check_region(id, dir_subset='data/subset'):
    file_list_subset_mod = glob.glob(os.path.join(dir_subset,id, '*MOD*.tif'))
    file_list_subset_img = glob.glob(os.path.join(dir_subset, id, '*IMG*.tif'))
    file_list_subset_img.sort()
    file_list_subset_mod.sort()
    for file in file_list_subset_mod:
        if file.replace('MOD','IMG') not in file_list_subset_img:
            print('Found missing '+id+file)

    for file in file_list_subset_img:
        array, _ = read_tiff(file)
        if np.nanmean(array) == 0 or np.isnan(np.nanmean(array)):
            os.remove(file)
            print(file + ' empty')
        if array.shape[0]!=5 and array.shape[0]!=2:
            if os.path.exists(file):
                os.remove(file)
            print(file+' band incomplete')

    for file in file_list_subset_mod:
        array, _ = read_tiff(file)
        if np.nanmean(array) == 0 or np.isnan(np.nanmean(array)):
            os.remove(file)
            print(file + ' empty')

    print('#Night images'+str(len(file_list_subset_img)-len(file_list_subset_mod)))


def sanity_check_nc_tif(dir_nc='E:\\viirs\\VNPNC', dir_tiff='G:\\viirs\\VNPIMGTIF'):
    logFile = 'log/sanity_check_' + time.strftime("%Y%m%d-%H%M%S") + '.log'
    logger = logging.getLogger('my_logger')
    handler = RotatingFileHandler(logFile, mode='a', maxBytes=50 * 1024 * 1024,
                                  backupCount=5, encoding=None, delay=False)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(handler)

    nc_list = glob.glob(os.path.join(dir_nc, '*', '*', '*'))
    nc_list.sort()
    for nc_file in nc_list:
        date = nc_file.split('/')[-3]
        DN = nc_file.split('/')[-2]
        capture_time = nc_file.split('/')[-1]
        tif_file = os.path.join(dir_tiff, date, DN, capture_time)
        nc_list_of_day =  glob.glob(os.path.join(nc_file, '*.nc'))
        intermediate_tif_list_of_day = glob.glob(os.path.join(nc_file, '*.tif'))
        intermediate_vrt_list_of_day = glob.glob(os.path.join(nc_file, '*.vrt'))
        tif_list_of_day = glob.glob(os.path.join(tif_file, '*.tif'))
        if len(nc_list_of_day) != 0 and len(tif_list_of_day) != 0:
            print('NC file of date {}, day_night {}, capture time {} is not needed please remove'.format(date, DN, capture_time))
            [os.remove(nc_file_of_day) for nc_file_of_day in nc_list_of_day]
            [os.remove(intermediate_tif_file_of_day) for intermediate_tif_file_of_day in intermediate_tif_list_of_day]
            [os.remove(intermediate_vrt_file_of_day) for intermediate_vrt_file_of_day in intermediate_vrt_list_of_day]
        elif len(nc_list_of_day) != 0 and len(tif_list_of_day) == 0:
            logger.info('NC file of date {}, day_night {}, capture time {} is not processed'.format(date, DN, capture_time))
        elif len(nc_list_of_day) == 0 and len(tif_list_of_day) != 0:
            print('TIF file of date {}, day_night {}, capture time {} is correctly created'.format(date, DN, capture_time))
            [os.remove(intermediate_tif_file_of_day) for intermediate_tif_file_of_day in intermediate_tif_list_of_day]
            [os.remove(intermediate_vrt_file_of_day) for intermediate_vrt_file_of_day in intermediate_vrt_list_of_day]
        else:
            if os.path.exists(tif_file):
                os.rmdir(tif_file)
            if os.path.exists(nc_file):
                os.rmdir(nc_file)
            print('date {}, day_night {}, capture time {} does not have any NC files or TIF files'.format(date, DN, capture_time))


def sanity_check_gee(dir_tiff):
    ee.Initialize()
    logFile = 'log/sanity_check_gee_' + time.strftime("%Y%m%d-%H%M%S") + '.log'

    logger = logging.getLogger('my_logger')
    handler = RotatingFileHandler(logFile, mode='a', maxBytes=50 * 1024 * 1024,
                                  backupCount=5, encoding=None, delay=False)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(handler)

    tif_list = glob.glob(os.path.join(dir_tiff, '21751303', '*.tif'))
    tif_list.sort()
    img_col = ee.ImageCollection('projects/proj5-dataset/assets/proj5_dataset')
    img_list_gee = img_col.aggregate_array('system:id').getInfo()
    img_list_gee = [img_gee.split('/')[-1] for img_gee in img_list_gee]
    for tif_file in tif_list:
        id = tif_file.split('/')[-2]
        date = tif_file.split('/')[-1][6:16]
        tif_time = tif_file.split('/')[-1][17:21]
        vnp_json = open(glob.glob(os.path.join('data/VNPL1', id, date, 'D', '*.json'))[0], 'rb')
        import json
        def get_name(json):
            return json.get('name').split('.')[2]

        vnp_time = list(map(get_name, json.load(vnp_json)['content']))
        if tif_time not in vnp_time or 'IMG' not in tif_file:
            continue
        tif_filename_gee = tif_file.split('/')[-2]+'_'+tif_file.split('/')[-1][:-4]
        if tif_filename_gee not in img_list_gee:
            print('GEE file not exist{}'.format(tif_filename_gee))
            logger.info('GEE file not exist_{}'.format(tif_filename_gee))

    print('finish')

if __name__=='__main__':
    # sanity_check(dir_nc='data/VNPNC', dir_tiff='data/VNPIMGTIF')
    # sanity_check_gee('data/subset')
    import pandas as pd
    year = '2021'
    filename = 'roi/us_fire_' + year + '_out_new.csv'
    df = pd.read_csv(filename)
    df = df.sort_values(by=['Id'])
    ids, start_dates, end_dates, lons, lats = df['Id'].values.astype(str), df['start_date'].values.astype(str), df['end_date'].values.astype(str), df['lon'].values.astype(float), df['lat'].values.astype(float)
    # ids = ['Alberta']
    for id in ids:
        print(id)
        sanity_check_region(id)