import argparse
import sys
from pathlib import Path
root_path = str(Path(__file__).resolve().parents[1]) + "/"
sys.path.insert(0,root_path)
from dags.modis_dag_day_na import convert_hdf_to_geotiff, download_files, upload_in_parallel
from easydict import EasyDict as edict
from utils import config
import ee

parser = argparse.ArgumentParser(description='Process some integers.')
parser.add_argument('-id', type=str, help='Product ID IMG or MOD',default="NA")
parser.add_argument('-sd', type=str, help='Start Date if mode is selected as roi',default="2024-06-20")
dir_data = Path(root_path + 'data/MOD09GA')
dir_tif = Path(root_path + 'data/MOD09GATIF')
args = parser.parse_args()
dn = ['D']
id = args.id
start_date = args.sd
utmzone = '4326'
if id == 'NA':
    hh_list = ['11']#['08' , '09', '10', '11', '12', '13', '14']
    vv_list = ['04']#['02' , '03', '04', '05']
elif id == 'EU':
    hh_list = ['17', '18', '19', '20', '21', '22', '23']
    vv_list = ['02', '03', '04', '05']

SOURCE = edict(config.modis_config['MOD09GA'])
products_id = SOURCE.products_id
collection_id = SOURCE.collection_id
asset_id = 'projects/ee-eo4wildfire/assets/MODIS_NA/'


def test_convert_hdf_to_tif():
    convert_hdf_to_geotiff(id, start_date, dir_data, dir_tif, SOURCE)

def test_download_files_hdf():
    download_files(id, start_date, dir_data, collection_id, products_id, hh_list=hh_list, vv_list =vv_list)

def test_gee_upload():
    upload_in_parallel(id, start_date, asset_id, filepath=root_path+'data/MOD09GATIF')

if __name__=='__main__':
    test_download_files_hdf()
    test_convert_hdf_to_tif()
    test_gee_upload()