import argparse
import sys
sys.path.insert(0,'/geoinfo_vol1/home/z/h/zhao2/LowResSatellitesService/')
from dags.viirs_dag_day_canada import download_files, read_and_project, upload_in_parallel

parser = argparse.ArgumentParser(description='Process some integers.')
parser.add_argument('-id', type=str, help='Product ID IMG or MOD')
parser.add_argument('-sd', type=str, help='Start Date if mode is selected as roi')
parser.add_argument('-ed', type=str, help='Start Date if mode is selected as roi')
args = parser.parse_args()
dir_json = '/geoinfo_vol1/home/z/h/zhao2/LowResSatellitesService/data/VNPL1'
dir_nc = '/geoinfo_vol1/home/z/h/zhao2/LowResSatellitesService/data/VNPNC'
dir_tif = '/geoinfo_vol1/home/z/h/zhao2/LowResSatellitesService/data/VNPIMGTIF'
dir_subset = '/geoinfo_vol1/home/z/h/zhao2/LowResSatellitesService/data/subset'
start_date = args.sd
end_date = args.ed
id = args.id
if id == 'CANADA':
    roi_arg = '-170,41,-41,73'
elif id == 'US':
    roi_arg = '-127,24,-66,50'
else:
    roi_arg = '-24,35,41,72'
asset_id = 'projects/ee-eo4wildfire/assets/VIIRS_Iband_Day_'+id+'/'
def test_download_files():
    download_files(id, roi_arg, start_date, end_date, dir_json, dir_nc)

def test_read_and_project():
    read_and_project(id, roi_arg, start_date, end_date, dir_nc, dir_tif, dir_subset)

def test_upload_in_parallel():
    upload_in_parallel(id, start_date, asset_id, filepath='data/subset')

if __name__=='__main__':
    test_download_files()
    test_read_and_project()
    test_upload_in_parallel()