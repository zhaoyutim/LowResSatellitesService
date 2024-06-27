from datetime import timedelta
from datetime import datetime

start_date = (datetime(2024, 6, 26))
default_args = {
    'owner': 'zhaoyutim',
    'start_date': start_date,
    'depends_on_past': False,
    'email': ['zhaoyutim@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'schedule_interval': '0 */8 * * *',
}

modis_config = {
    'VNP09GA': {
        "products_id": "VNP09GA",
        "collection_id": '5000',
        "eeImgColName": "VIIRS_NRT",
        "format": '.h5',
        "BANDS": ["M3", "M4", "M5", "M7", "M10", "M11", "QF2"]
    },

    'VNP09_NRT': {
        "products_id": "VNP09_NRT",
        "collection_id": '5000',
        "eeImgColName": "VIIRS_NRT",
        "format": '.hdf',
        "BANDS": [
            "375m Surface Reflectance Band I1",
            "375m Surface Reflectance Band I2",
            "375m Surface Reflectance Band I1"
        ]
    },

    'MOD09GA': {
        "products_id": "MOD09GA",
        "collection_id": '61',
        "eeImgColName": "MODIS_NRT",
        "format": '.hdf',
        "BANDS": [
            "sur_refl_b01_1",
            "sur_refl_b02_1",
            # "sur_refl_b03_1",
            # "sur_refl_b04_1",
            "sur_refl_b07_1"
        ]
    },

    'MOD02HKM': {
        "products_id": "MOD02HKM",
        "collection_id": '61',
        "eeImgColName": "MODIS_NRT",
        "format": '.hdf',
        "BANDS": ["sur_refl_b01_1", "sur_refl_b02_1", "sur_refl_b03_1", "sur_refl_b04_1", "sur_refl_b07_1"]
    },
}

auth_token = 'eyJ0eXAiOiJKV1QiLCJvcmlnaW4iOiJFYXJ0aGRhdGEgTG9naW4iLCJzaWciOiJlZGxqd3RwdWJrZXlfb3BzIiwiYWxnIjoiUlMyNTYifQ.eyJ0eXBlIjoiVXNlciIsInVpZCI6InpoYW95dXRpbSIsImV4cCI6MTcyMzI5ODY0OSwiaWF0IjoxNzE4MTE0NjQ5LCJpc3MiOiJFYXJ0aGRhdGEgTG9naW4ifQ.JR19mk5uizk0Ar_82bPHA5BihSEgEm7gjJVUWzztoB3v_X7me3cZesrOdBEOTx6qcsYYhUzQuIGlguh_J0va2n7jFn9xMhZCMbBo3yL-Pu58vKJOTk0DVWfgQDI162LRsGNwbDUiS8CeiMg7N-HScBEel1OZA4b9JVI0-nzbsvvgH9w0nOyWnc9bPIIvSWUKazeZ-9sgG6XhRIGLVrwJt5GzWmeNFNIw8B04vJ_YN9EwPtd_vuKmadtEfvOPbiNTqoOK_b4FoeB4-hRO296vuOFeIS4LgVc2zAwxeVTWySFpA4P2FFbTFv_jReVMIVGEaceWhhMDbbxDW7KDyn2afw'
ee_path = '/home/a/a/aadelow/miniforge3/envs/lrss/bin/earthengine'
project_name = 'ee-eo4wildfire'