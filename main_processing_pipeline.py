# -*- coding: utf-8 -*-
import datetime
import multiprocessing
import pandas as pd
from LaadsDataHandler.laads_client import LaadsClient
from ProcessingPipeline.processing_pipeline import Pipeline
if __name__ == '__main__':
    year = '2019'
    filename = 'roi/us_fire_' + year + '_out_new.csv'
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
        with multiprocessing.Pool(processes=4) as pool:
            for k in range(duration.days):
                date = (datetime.datetime.strptime(start_date, '%Y-%m-%d') + datetime.timedelta(k)).strftime('%Y-%m-%d')
                result = pool.apply_async(pipeline.processing, (date, id, roi, utmzone, 'MOD', 'G:\\viirs\\VNPNC',
                                                                'C:\\Users\\Yu\\Desktop\\viirs\\VNPMODTIF', 'C:\\Users\\Yu\\Desktop\\viirs\\subset'))
                results.append(result)
            results = [result.get() for result in results if result is not None]
