# -*- coding: utf-8 -*-
import datetime
import multiprocessing
import pandas as pd
from LaadsDataHandler.laads_client import LaadsClient
from ProcessingPipeline.processing_pipeline import Pipeline
if __name__ == '__main__':
    year = '2020'
    filename = 'roi/us_fire_' + year + '_out.csv'
    df = pd.read_csv(filename)
    df = df.head(5)
    utmzone = '32610'
    laads_client = LaadsClient()
    collection_id = '5200' # 5110 for VNP series
    products_id = ['VNP02IMG', 'VNP03IMG'] #['VJ102IMG', 'VJ103IMG'] ['VNP02MOD', 'VNP03MOD'], ['VNP02IMG', 'VNP03IMG'], ['VJ102MOD', 'VJ103MOD']
    ids, start_dates, end_dates, lons, lats = df['Id'].values.astype(str), df['start_date'].values.astype(str), df['end_date'].values.astype(str), df['lon'].values.astype(float), df['lat'].values.astype(float)
    for i, id in enumerate(ids):
        lon, lat, start_date, end_date = lons[i], lats[i], start_dates[i], end_dates[i]
        roi_size = 1
        roi = [lon - roi_size, lat - roi_size, lon + roi_size, lat + roi_size]
        duration = datetime.datetime.strptime(end_date, '%Y-%m-%d')-datetime.datetime.strptime(start_date, '%Y-%m-%d')
        area_of_interest = 'W'+str(roi[0])+' '+'N'+str(roi[3])+' '+'E'+str(roi[2])+' '+'S'+str(roi[1])
        procs = []
        print('Currently processing id {}'.format(id))
        for d in range(duration.days):
            date = (datetime.datetime.strptime(start_date, '%Y-%m-%d')+datetime.timedelta(d)).strftime('%Y-%m-%d')
            proc = multiprocessing.Process(target=laads_client.query_filelist_with_date_range_and_area_of_interest,
                                           args=(id, date, area_of_interest, products_id, ['D'], 'data/VNPL1', collection_id))
            procs.append(proc)
            proc.start()
        for proc in procs:
            proc.join()
        procs_download = []
        for d in range(zduration.days):
            date = (datetime.datetime.strptime(start_date, '%Y-%m-%d')+datetime.timedelta(d)).strftime('%Y-%m-%d')
            proc_download = multiprocessing.Process(target=laads_client.download_files_to_local_based_on_filelist,
                                           args=(id, date, products_id, ['D'], 'data/VNPL1', collection_id))
            procs_download.append(proc_download)
            proc_download.start()
        for proc_download in procs_download:
            proc_download.join()

        # for i in range(3):
        #     date = (datetime.datetime.strptime(start_date, '%Y-%m-%d')+datetime.timedelta(i)).strftime('%Y-%m-%d')
        #     pipeline = Pipeline()
        #     pipeline.processing(date, roi, utmzone, dir_data='data/VNPL1')