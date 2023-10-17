# -*- coding: utf-8 -*-
import argparse
import datetime
import glob
import multiprocessing
import os
import platform

import pandas as pd
from ProcessingPipeline.processing_pipeline import Pipeline
from utils.utils import get_tasks, main_process_wrapper

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('-pid', type=str, help='Product ID IMG or MOD')
    parser.add_argument('-mode', type=str, help='Mode to be selected between roi based or csv based')
    parser.add_argument('-y', type=str, help='Year if mode is selected as csv')
    parser.add_argument('-dn', type=str, help='Daynight')
    parser.add_argument('-roi', type=str, help='lon_min,lat_min,lon_max,lat_max', default=None)
    parser.add_argument('-sd', type=str, help='Start Date if mode is selected as roi', default=None)
    parser.add_argument('-ed', type=str, help='Start Date if mode is selected as roi', default=None)
    args = parser.parse_args()
    mode = args.mode
    year = args.y
    roi_arg = args.roi
    product_id = args.pid
    dn = []
    for i in range(len(args.dn)):
        dn.append(args.dn[i])
    utmzone = '4326'
    filename = 'roi/us_fire_' + year + '_out_new.csv'

    if platform.system()=='Windows':
        dir_nc = 'G:\\viirs\\VNPNC'
        dir_tif = 'C:\\Users\\Yu\\Desktop\\viirs\\VNPIMGTIF'
        dir_subset = 'C:\\Users\\Yu\\Desktop\\viirs\\subset'

    elif platform.system()=='Darwin':
        dir_nc = 'G:\\viirs\\VNPNC'
        dir_tif = 'C:\\Users\\Yu\\Desktop\\viirs\\VNPIMGTIF'
        dir_subset = 'C:\\Users\\Yu\\Desktop\\viirs\\subset'

    else:
        dir_nc = 'data/VNPNC'
        dir_tif = 'data/VNP'+product_id+'TIF'
        dir_subset = 'data/subset'



    if mode == 'csv':
        df = pd.read_csv(filename)
        df = df.sort_values(by=['Id'])
        ids, start_dates, end_dates, lons, lats = df['Id'].values.astype(str), df['start_date'].values.astype(str), df[
            'end_date'].values.astype(str), df['lon'].values.astype(float), df['lat'].values.astype(float)
        for i, target_id in enumerate(ids):
            if not os.path.exists(dir_subset):
                os.mkdir(dir_subset)
            if not os.path.exists(os.path.join(dir_subset, target_id)):
                os.mkdir(os.path.join(dir_subset, target_id))
            lon, lat, start_date, end_date = lons[i], lats[i], start_dates[i], end_dates[i]
            roi_size = 1
            roi = [lon - roi_size, lat - roi_size, lon + roi_size, lat + roi_size]
            duration = datetime.datetime.strptime(end_date, '%Y-%m-%d') - datetime.datetime.strptime(start_date, '%Y-%m-%d')
            tasks = get_tasks(start_date, duration, target_id, roi, dn, utmzone, product_id, dir_nc, dir_tif, dir_subset)
            with multiprocessing.Pool(processes=4) as pool:
                results = list(pool.imap_unordered(main_process_wrapper, tasks))
            print(results)
    elif mode == 'error':
        df = pd.read_csv(filename)
        df = df.sort_values(by=['Id'])
        with open('log/error', 'r') as f:
            file = f.read().split('\n')

        def get_id(dir_str):
            return dir_str.split('/')[1]
        target_ids = list(map(get_id, file))

        def get_date(dir_str):
            return dir_str.split('/')[-1][:10]
        target_dates = list(map(get_date, file))
        for i, target_id in enumerate(target_ids):

            if not os.path.exists(dir_subset):
                os.mkdir(dir_subset)
            if not os.path.exists(os.path.join(dir_subset, target_id)):
                os.mkdir(os.path.join(dir_subset, target_id))

            df_row = df[df.Id == int(target_id)]
            lon, lat, start_date = df_row['lon'].values.astype(float), df_row['lat'].values.astype(float), target_dates[i]
            roi_size = 1
            day_night = 'D'
            roi = [lon - roi_size, lat - roi_size, lon + roi_size, lat + roi_size]
            duration = datetime.timedelta(days=1)
            tif_list = glob.glob(os.path.join('data/VNPIMGTIF', start_date, day_night, 'VNPIMG'+start_date+'*.tif'))
            for tif_file in tif_list:
                os.remove(tif_file)
            subset_list = glob.glob(os.path.join('data/subset', target_id, 'VNPIMG'+start_date+'*.tif'))
            for subset_file in subset_list:
                os.remove(subset_file)

            tasks = get_tasks(start_date, duration, target_id, roi, dn, utmzone, product_id, dir_nc, dir_tif, dir_subset)
            with multiprocessing.Pool(processes=4) as pool:
                results = list(pool.imap_unordered(main_process_wrapper, tasks))
            print(results)
    elif mode == 'roi':
        start_date = args.sd
        end_date = args.ed
        roi = [float(roi_arg.split(',')[0]), float(roi_arg.split(',')[1]), float(roi_arg.split(',')[2]),
               float(roi_arg.split(',')[3])]
        duration = datetime.datetime.strptime(end_date, '%Y-%m-%d') - datetime.datetime.strptime(start_date, '%Y-%m-%d')
        tasks=[]
        
        # Changes made here (TODO: Change this to whatever was there previously)
        target_id= 'Alberta'
        if not os.path.exists(os.path.join(dir_subset, target_id)):
            os.mkdir(os.path.join(dir_subset, target_id))
        tasks = get_tasks(start_date, duration, target_id, roi, dn, utmzone, product_id, dir_nc, dir_tif, dir_subset)

        with multiprocessing.Pool(processes=4) as pool:
            results = list(pool.imap_unordered(main_process_wrapper, tasks))
        print(results)
    elif mode == 'roi_from_csv':
        fire_name = args.y
        if fire_name not in ['quebec_fire', 'alberta_fire']:
            raise 'For roi_from_csv mode quebec_fire or alberta_fire is expected from year arguments'
        filename = 'roi/'+fire_name+'.csv'
        df = pd.read_csv(filename)
        ids, start_dates, end_dates, bottom_lefts, top_rights = df['system:index'].values.astype(str), df['start_date'].values.astype(str), df['end_date'].values.astype(str), df['bottom_left'].values.astype(str), df['top_right'].values.astype(str)
        for i, id in enumerate(ids):
            roi = (bottom_lefts[i] + top_rights[i]).replace('[', '').split(']')[0].split(', ')+(bottom_lefts[i] + top_rights[i]).replace('[', '').split(']')[1].split(', ')
            start_date = start_dates[i].split('T')[0]
            end_date = end_dates[i].split('T')[0]
            duration = datetime.datetime.strptime(end_date, '%Y-%m-%d') - datetime.datetime.strptime(start_date, '%Y-%m-%d')
            area_of_interest = 'W' + str(roi[0]) + ' ' + 'N' + str(roi[3]) + ' ' + 'E' + str(roi[2]) + ' ' + 'S' + str(roi[1])
            id = fire_name+'_'+id
            print('Currently processing id {}'.format(id))
            if not os.path.exists(os.path.join(dir_subset, id)):
                os.mkdir(os.path.join(dir_subset, id))
            tasks = get_tasks(start_date, duration, id, roi, dn, utmzone, product_id, dir_nc, dir_tif,
                              dir_subset)

            with multiprocessing.Pool(processes=4) as pool:
                results = list(pool.imap_unordered(main_process_wrapper, tasks))
            print(results)
