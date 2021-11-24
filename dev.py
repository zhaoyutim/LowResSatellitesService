import datetime

from LaadsDataHandler.laads_client import LaadsClient
from ProcessingPipeline.processing_pipeline import Pipeline

if __name__ == '__main__':
    start_date = '2020-08-22'
    roi = [-130.23, 32.56, -113.10, 60.01]
    # roi = [13.38,61.55,15.59,62.07] #xmin, ymin, xmax, ymax
    for i in range(1):
        date = (datetime.datetime.strptime(start_date, '%Y-%m-%d')+datetime.timedelta(i)).strftime('%Y-%m-%d')
        area_of_interest = 'W'+str(roi[0])+' '+'N'+str(roi[3])+' '+'E'+str(roi[2])+' '+'S'+str(roi[1])
        laads_client = LaadsClient()
        laads_client.query_filelist_with_date_range_and_area_of_interest(date, area_of_interest=area_of_interest)
        laads_client.download_files_to_local_based_on_filelist(date)
    for i in range(1):
        date = (datetime.datetime.strptime(start_date, '%Y-%m-%d')+datetime.timedelta(i)).strftime('%Y-%m-%d')
        pipeline = Pipeline()
        pipeline.processing(date, roi)