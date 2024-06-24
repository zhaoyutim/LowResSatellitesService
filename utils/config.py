from datetime import timedelta
from datetime import datetime

start_date = (datetime(2024, 6, 24))
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

modis_token = 'eyJ0eXAiOiJKV1QiLCJvcmlnaW4iOiJFYXJ0aGRhdGEgTG9naW4iLCJzaWciOiJlZGxqd3RwdWJrZXlfb3BzIiwiYWxnIjoiUlMyNTYifQ.eyJ0eXBlIjoiVXNlciIsInVpZCI6InpoYW95dXRpbSIsImV4cCI6MTcyMjAxNjA5MywiaWF0IjoxNzE2ODMyMDkzLCJpc3MiOiJFYXJ0aGRhdGEgTG9naW4ifQ.5NTvTwZnH4iKpUYSOak-WgLoQ_XQag-6L2qaphQDze5KxJGPnAV5JIdVAtN78dm2UftRG0-MoHujELUZ_acsr3Gtsw1TlByhGEuh5F9WgWeVl55l37jlzDZcWWdsjLLdpf7SfgvnP-SAIMUAbeBlo1wIBKJUGqEX4qaxBVg5AOZlxZo6o2kOBI1UCxhhOTjQE8r0Na3dOI6oulEMvpmoGvphj0IabnrNCdCGctni_45PhKHnH38OBiErnIsWH4Wo8AVmXAOFmbZDpytOjuKQwpELVm2VC90nYNcOJhArPxvKjz_ZAdU8Rr0kYDImTfbgCZXzohvsMj2VcBqnx_S2Mg'
ee_path = '/home/a/a/aadelow/miniforge3/envs/lrss/bin/earthengine'
project_name = 'ee-eo4wildfire'