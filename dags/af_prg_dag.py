import sys
from pathlib import Path
root_path = str(Path(__file__).resolve().parents[1]) + "/"
sys.path.insert(0,root_path)

from airflow import DAG
from airflow.operators.python import PythonOperator
from utils import config
import ee
import utils
from datetime import datetime

ee.Authenticate()
ee.Initialize(project=utils.config.project_name)

from_date = "2024-04-01"
to_date = datetime.today().strftime('%Y-%m-%d')
#to_date = "2024-05-05"

sources = ["VIIRS","MODIS"]
ids = ["CA","US","EU"]
regions = [ee.Geometry.Polygon([[-171.85273276498725, 71.77440635086317],[-171.85273276498725, 25.88033295113415],[-52.673045264987266, 25.88033295113415],[-52.673045264987266, 71.77440635086317]]), #CA
           ee.Geometry.Polygon([[-140, 40],[-140, 10],[-40, 10],[-40, 40]]), #US
           ee.Geometry.Polygon([[[-17.504687500000014, 62.86752265950611],[-17.504687500000014, 28.21215701660283],[44.01874999999998, 28.21215701660283],[44.01874999999998, 62.86752265950611]]])] #EU

def convert_active_fire(id, source, from_date, to_date, path, to_asset_id,region):
    from_date_millis = ee.Date(from_date).millis()
    to_date_millis = ee.Date(to_date).millis()

    fc = ee.FeatureCollection(path) \
    .filterBounds(region) \
    .filter(ee.Filter.gte('ACQ_DATE', ee.Date(from_date).millis().getInfo())) \
    .map(lambda feature: feature.buffer(500).bounds())

    def add_numeric_date(feature):
        acq_date = ee.Date(feature.get('ACQ_DATE'))
        diff_days = acq_date.difference(from_date, 'day')
        return feature.set('numeric_date', diff_days)

    fc_with_numeric_date = fc.map(add_numeric_date)

    prg_regional = fc_with_numeric_date.reduceToImage(
        properties=['numeric_date'],
        reducer=ee.Reducer.first()
    ).rename(['numeric_date'])

    prg_regional = prg_regional.set({
        'system:time_start': from_date_millis,
        'system:time_end': to_date_millis
    })

    asset_info = ee.data.getInfo(to_asset_id + to_date)
    if asset_info:
        ee.data.deleteAsset(to_asset_id + to_date)
    
    export_task = ee.batch.Export.image.toAsset(
        image=prg_regional,
        description='prg_regional_export_'+source+'_'+id,
        assetId=to_asset_id + to_date,
        scale=500,  
        region=region,
        maxPixels=1e9
    )

    export_task.start()
    print(export_task.status())    

for source in sources:
    dag = DAG(
        f'{source}_Prg_dag',
        default_args=config.default_args,
        schedule_interval='0 11 * * *',
        description='A DAG for converting {source} Active Fire to progression in gee'
    )
    with dag:
        for i in range(len(ids)):
            convert_task_viirs_ca = PythonOperator(
                task_id=f'convert_task_{ids[i]}',
                python_callable=convert_active_fire,
                op_kwargs={
                    'id': ids[i],
                    'source':source,
                    'from_date': from_date,
                    'to_date': to_date,
                    'path': f"projects/ee-eo4wildfire/assets/{source}_AF_2024",
                    'to_asset_id': f"projects/ee-eo4wildfire/assets/progressions/{source}_PRG_{ids[i]}/",
                    'region':regions[i]},
            )