import datetime

from LaadsDataHandler.laads_client import LaadsClient
from ProcessingPipeline.processing_pipeline import Pipeline

if __name__ == '__main__':
    start_date = '2020-08-08'
    for i in range(5):
        date = (datetime.datetime.strptime(start_date, '%Y-%m-%d')+datetime.timedelta(i)).strftime('%Y-%m-%d')
        laads_client = LaadsClient()
        laads_client.query_filelist_with_date_range_and_area_of_interest(date)
        laads_client.download_files_to_local_based_on_filelist(date)
    for i in range(5):
        date = (datetime.datetime.strptime(start_date, '%Y-%m-%d')+datetime.timedelta(i)).strftime('%Y-%m-%d')
        pipeline = Pipeline()
        pipeline.processing(date)