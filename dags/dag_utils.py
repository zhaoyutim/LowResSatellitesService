import ee
import utils.config

def auth():
    ee.Authenticate()
    ee.Initialize(project=utils.config.project_name)

def to_feature_view(asset_id):
    auth()
    collection = ee.FeatureCollection(asset_id)
    task = ee.batch.Export.table.toFeatureView(
        collection=collection,
        assetId= asset_id+'_FEATUREVIEW',
        description= 'FEATUREVIEW OF ASSET',
        maxFeaturesPerTile= 100
        )    
    task.start()
    print(task.status())