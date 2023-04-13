# -*- coding: utf-8 -*-
import argparse
import datetime
import multiprocessing
import os
import platform

import pandas as pd
from ProcessingPipeline.processing_pipeline import Pipeline
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('-pid', type=str, help='Product ID IMG or MOD')
    parser.add_argument('-mode', type=str, help='Mode to be selected between roi based or csv based')
    parser.add_argument('-y', type=str, help='Year if mode is selected as csv')
    parser.add_argument('-roi', type=str, help='lon_min,lat_min,lon_max,lat_max')
    parser.add_argument('-sd', type=str, help='Start Date if mode is selected as roi')
    parser.add_argument('-ed', type=str, help='Start Date if mode is selected as roi')
    args = parser.parse_args()
    mode = args.mode
    year = args.y
    roi_arg = args.roi
    product_id = args.pid
    utmzone = '4326'
    filename = 'roi/us_fire_' + year + '_out_new.csv'
    pipeline = Pipeline()
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

    def main_process_wrapper(args):
        return pipeline.processing(*args)

    def get_tasks(start_date, duration, id, roi, utmzone, product_id, dir_nc, dir_tif, dir_subset):
        tasks = []
        for k in range(duration.days):
            tasks.append(
                (
                    (datetime.datetime.strptime(start_date, '%Y-%m-%d') + datetime.timedelta(k)).strftime('%Y-%m-%d'),
                    id, roi, utmzone, product_id, dir_nc, dir_tif, dir_subset
                )
            )
        return tasks

    if mode == 'csv':
        df = pd.read_csv(filename)
        df = df.sort_values(by=['Id'])
        ids, start_dates, end_dates, lons, lats = df['Id'].values.astype(str), df['start_date'].values.astype(str), df[
            'end_date'].values.astype(str), df['lon'].values.astype(float), df['lat'].values.astype(float)
        for i, id in enumerate(ids):
            if not os.path.exists(dir_subset):
                os.mkdir(dir_subset)
            if not os.path.exists(os.path.join(dir_subset, id)):
                os.mkdir(os.path.join(dir_subset, id))
            lon, lat, start_date, end_date = lons[i], lats[i], start_dates[i], end_dates[i]
            roi_size = 1
            roi = [lon - roi_size, lat - roi_size, lon + roi_size, lat + roi_size]
            duration = datetime.datetime.strptime(end_date, '%Y-%m-%d') - datetime.datetime.strptime(start_date, '%Y-%m-%d')
            tasks = get_tasks(start_date, duration, id, roi, utmzone, product_id, dir_nc, dir_tif, dir_subset)

    elif mode == 'roi':
        start_date = args.sd
        end_date = args.ed
        roi = [float(roi_arg.split(',')[0]), float(roi_arg.split(',')[1]), float(roi_arg.split(',')[2]),
               float(roi_arg.split(',')[3])]
        duration = datetime.datetime.strptime(end_date, '%Y-%m-%d') - datetime.datetime.strptime(start_date, '%Y-%m-%d')
        tasks=[]
        id='ASIA'
        if not os.path.exists(os.path.join(dir_subset, id)):
            os.mkdir(os.path.join(dir_subset, id))
        tasks = get_tasks(start_date, duration, id, roi, utmzone, product_id, dir_nc, dir_tif, dir_subset)

    with multiprocessing.Pool(processes=4) as pool:
        results = list(pool.imap_unordered(main_process_wrapper, tasks))
    print(results)
