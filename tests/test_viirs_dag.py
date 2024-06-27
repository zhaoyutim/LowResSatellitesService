import argparse
import sys
from pathlib import Path
root_path = str(Path(__file__).resolve().parents[1]) + "/"
sys.path.insert(0,root_path)

from dags.viirs_dag_day_canada import download_files, read_and_project, upload_in_parallel

parser = argparse.ArgumentParser(description='Process some integers.')
parser.add_argument('-id', type=str, help='Product ID IMG or MOD',default='CANADA')
parser.add_argument('-sd', type=str, help='Start Date if mode is selected as roi',default='2024-06-08')
parser.add_argument('-ed', type=str, help='End Date if mode is selected as roi', default='2024-06-09')
args = parser.parse_args()

dir_json = root_path + 'data/VNPL1'
dir_nc = root_path + 'data/VNPNC'
dir_tif = root_path + 'data/VNPIMGTIF'
dir_subset = root_path + 'data/subset'

start_date = args.sd
end_date = args.ed
id = args.id

if id == 'CANADA':
    roi_arg = '-170,41,-41,73'
elif id == 'US':
    roi_arg = '-127,24,-66,50'

asset_id = 'projects/ee-eo4wildfire/assets/VIIRS_Iband_Day_'+id+'/'
def test_download_files():
    download_files(id, roi_arg, start_date, end_date, dir_json, dir_nc)

def test_read_and_project():
    read_and_project(id, roi_arg, start_date, end_date, dir_nc, dir_tif, dir_subset, dir_json)

def test_upload_in_parallel():
    upload_in_parallel(id, start_date, asset_id, filepath=root_path+'data/subset')

if __name__=='__main__':
    test_download_files()
    test_read_and_project()
    test_upload_in_parallel()