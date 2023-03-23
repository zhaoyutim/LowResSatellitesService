# -*- coding: utf-8 -*-
import datetime
import multiprocessing
import platform

import pandas as pd
from LaadsDataHandler.laads_client import LaadsClient
from ProcessingPipeline.processing_pipeline import Pipeline
if __name__ == '__main__':
    year = '2017'
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
        dir_nc = 'G:\\viirs\\VNPNC'
        dir_tif = 'C:\\Users\\Yu\\Desktop\\viirs\\VNPIMGTIF'
        dir_subset = 'C:\\Users\\Yu\\Desktop\\viirs\\subset'
    df = pd.read_csv(filename)
    utmzone = '4326'
    pipeline = Pipeline()
    df = df.sort_values(by=['Id'])
    ids, start_dates, end_dates, lons, lats = df['Id'].values.astype(str), df['start_date'].values.astype(str), df[
        'end_date'].values.astype(str), df['lon'].values.astype(float), df['lat'].values.astype(float)
    for i, id in enumerate(ids):
        lon, lat, start_date, end_date = lons[i], lats[i], start_dates[i], end_dates[i]
        roi_size = 1
        roi = [lon - roi_size, lat - roi_size, lon + roi_size, lat + roi_size]
        duration = datetime.datetime.strptime(end_date, '%Y-%m-%d') - datetime.datetime.strptime(start_date, '%Y-%m-%d')
        results = []
        with multiprocessing.Pool(processes=8) as pool:
            for k in range(duration.days):
                date = (datetime.datetime.strptime(start_date, '%Y-%m-%d') + datetime.timedelta(k)).strftime('%Y-%m-%d')
                result = pool.apply_async(pipeline.processing, (date, id, roi, utmzone, 'IMG', dir_nc, dir_tif, dir_subset))
                results.append(result)
            results = [result.get() for result in results if result is not None]
