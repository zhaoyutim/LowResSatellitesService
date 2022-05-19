# -*- coding: utf-8 -*-
import datetime
import os

import yaml

from LaadsDataHandler.laads_client import LaadsClient
from ProcessingPipeline.processing_pipeline import Pipeline
with open("roi/configuration.yml", "r", encoding="utf8") as f:
    config = yaml.load(f, Loader=yaml.FullLoader)
if __name__ == '__main__':
    locations = ['Africa']
    collection_id = '5110'
    utmzone = '32736'
    for location in locations:
        start_date = config.get(location).get('start').strftime('%Y-%m-%d')
        lat = config.get(location).get('latitude')
        lon = config.get(location).get('longitude')
        size = 2
        roi = [lon-size, lat-size, lon+size, lat+size]    # roi = [13.38,61.55,15.59,62.07] #xmin, ymin, xmax, ymax
        # for i in range(1):
        #     date = (datetime.datetime.strptime(start_date, '%Y-%m-%d')+datetime.timedelta(i)).strftime('%Y-%m-%d')
        #     area_of_interest = 'W'+str(roi[0])+' '+'N'+str(roi[3])+' '+'E'+str(roi[2])+' '+'S'+str(roi[1])
        #     laads_client = LaadsClient()
        #     laads_client.query_filelist_with_date_range_and_area_of_interest(date, products_id=['VNP02IMG', 'VNP03IMG'], data_path='data\VNPL1', collection_id=collection_id, area_of_interest=area_of_interest)
        #     laads_client.download_files_to_local_based_on_filelist(date, products_id=['VNP02IMG', 'VNP03IMG'], collection_id=collection_id, data_path='data\VNPL1')
        for i in range(1):
            date = (datetime.datetime.strptime(start_date, '%Y-%m-%d')+datetime.timedelta(i)).strftime('%Y-%m-%d')
            pipeline = Pipeline()
            pipeline.processing(date, roi, utmzone, dir_data=os.path.join('data','VNPL1'))