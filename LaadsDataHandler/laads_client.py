import datetime
import json
import os
import shutil
from pprint import pprint

import requests


class LaadsClient:

    def __init__(self):
        self.laads_query_api_link = 'https://ladsweb.modaps.eosdis.nasa.gov/api/v1/files/'
        self.download_base_link = 'https://ladsweb.modaps.eosdis.nasa.gov/archive/allData/'
        self.header = {
            "X-Requested-With": "XMLHttpRequest",
            'Authorization': 'Bearer emhhb3l1dGltOmVtaGhiM2wxZEdsdFFHZHRZV2xzTG1OdmJRPT06MTYzMzk0NzU0NTphZmRlYWY2MjE2ODg0MjQ5MTEzNmE3MTE4MzZkOWYxYjg3MWQzNWMz'}

    def query_filelist_with_date_range_and_area_of_interest(self, date, data_path='../data/VNPL1', collection_id='5200', area_of_interest='x-129.5y56.2,x-110.4y31.7'):
        products_id = ['VNP02IMG', 'VNP03IMG']
        for i in range(2):
            product_id = products_id[i]
            download_link = self.laads_query_api_link\
                            + 'product='+product_id\
                            + '&collection='+collection_id\
                            + '&dateRanges=' + date \
                            + '&areaOfInterest=' + area_of_interest

            response = requests.get(download_link, headers=self.header)
            if response.status_code != 200:
                raise ConnectionRefusedError
            else:
                if not os.path.exists(data_path + '/' + date):
                    os.mkdir(data_path + '/' + date)
                with open(data_path + '/' + date + '/' + date + '_'+product_id+'.json', 'wb') as outf:
                    outf.write(response.content)
                print('New ' + product_id +' file list for day '+date+' created')

    def download_files_to_local_based_on_filelist(self, date, data_path='../data/VNPL1'):
        products_id = ['VNP02IMG', 'VNP03IMG']

        date_ndays = (datetime.datetime.strptime(date, '%Y-%m-%d')-datetime.datetime.strptime(date[:4]+'-01-01', '%Y-%m-%d')).days+1
        for i in range(2):
            product_id = products_id[i]
            vnp_json = open(data_path + '/' + date + '/' + date + '_'+product_id+'.json', )
            vnp_list = json.load(vnp_json)
            download_link = 'https://ladsweb.modaps.eosdis.nasa.gov/archive/allData/5200/' + product_id +'/' + date[:4] + '/'

            print('There are ' + str(vnp_list.__len__()) + ' ' + product_id +' files to download in total.')
            for vnp_file in vnp_list:
                vnp_file = vnp_list[vnp_file]

                vnp_name = vnp_file['name']
                time_captured = vnp_name.split('.')[2]
                save_path = data_path.replace('VNPL1', 'VNPIMGTIF')
                path_to_geotiff_day = save_path + '/' + date + '/' + time_captured + '_D'
                path_to_geotiff_night = save_path + '/' + date + '/' + time_captured + '_N'

                if not os.path.exists(save_path + '/' + date + '/' + time_captured):
                    os.mkdir(save_path + '/' + date + '/' + time_captured)

                print(time_captured)
                vnp_link = download_link + str(date_ndays) + '/' + vnp_name
                # Keep a clean directory before downloading
                if not os.path.exists(data_path + '/' + date + '/' + time_captured):
                    os.mkdir(data_path + '/' + date + '/' + time_captured)

                if not os.path.exists(data_path + '/' + date + '/' + time_captured + '/' + vnp_name):
                    print("Downloading netCDF files " + vnp_name.split('.')[1] + vnp_name.split('.')[
                        2] + " from Remote server")
                    # shutil.rmtree(data_path + '/' + date + '/' + time_captured)
                    wget_command_vnp = "wget " + vnp_link + " --header \"Authorization: Bearer emhhb3l1dGltOmVtaGhiM2wxZEdsdFFHZHRZV2xzTG1OdmJRPT06MTYzMzk0NzU0NTphZmRlYWY2MjE2ODg0MjQ5MTEzNmE3MTE4MzZkOWYxYjg3MWQzNWMz\" -P " + data_path + '/' + date + '/' + time_captured
                    os.system(wget_command_vnp)

if __name__ == '__main__':
    date = '2021-07-14'
    laads_client = LaadsClient()
    laads_client.query_filelist_with_date_range_and_area_of_interest(date)
    laads_client.download_files_to_local_based_on_filelist(date)