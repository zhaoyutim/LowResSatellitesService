from datetime import timedelta
from datetime import datetime

start_date = (datetime(2023, 7, 3))
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

modis_token = 'eyJ0eXAiOiJKV1QiLCJvcmlnaW4iOiJFYXJ0aGRhdGEgTG9naW4iLCJzaWciOiJlZGxqd3RwdWJrZXlfb3BzIiwiYWxnIjoiUlMyNTYifQ.eyJ0eXBlIjoiVXNlciIsInVpZCI6InpoYW95dXRpbSIsImV4cCI6MTY5Mjk0ODY4OSwiaWF0IjoxNjg3NzY0Njg5LCJpc3MiOiJFYXJ0aGRhdGEgTG9naW4ifQ.8VcF6eI2baAURBcKZn1Cca9xTTt5bMxMCnF9bfMaqH6GLiDMd-j3f35aTJikF1amrkRq_Mq9L-KFEUBdkOn-Qn3BFiLsxIvKIEjtvl02mGigYExtK5trxJOi4Vm3NBeZIGBjiFdOU1kjmJl-uu9o_lnWSH7xQBQc6uEJ8zrX3Z31nnel8DiwZIv1GN5R5ElUqce38oYk7xyymfzeBx94tEUi084gwuQtwTOvAc_Xly0ZQcBidJh_UKuZKCbxBgPOmwTlHPdrjN-FofSRIXx8M8CdMomJV0h9_SGxikF1r0dV-oPYDxA40vNhNMUepYYd1iGkeIFYwZUfZ5P87upZ-g'
#modis_token = 'emhhb3l1dGltOmVtaGhiM2wxZEdsdFFHZHRZV2xzTG1OdmJRPT06MTYyNjQ0MTQyMTphMzhkYTcwMzc5NTg1M2NhY2QzYjY2NTU0ZWFkNzFjMGEwMTljMmJj'