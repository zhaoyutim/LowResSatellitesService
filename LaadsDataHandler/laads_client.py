import datetime
import json
import os
import subprocess
from simple_file_checksum import get_checksum

import requests


class LaadsClient:

    def __init__(self):
        self.laads_query_api_link = 'https://ladsweb.modaps.eosdis.nasa.gov/api/v2/content/details'
        self.download_base_link = 'https://ladsweb.modaps.eosdis.nasa.gov'
        self.header = {
            "X-Requested-With": "XMLHttpRequest",
            'Authorization': 'Bearer emhhb3l1dGltOmVtaGhiM2wxZEdsdFFHZHRZV2xzTG1OdmJRPT06MTYzMzk0NzU0NTphZmRlYWY2MjE2ODg0MjQ5MTEzNmE3MTE4MzZkOWYxYjg3MWQzNWMz'}

    def runcmd(self, cmd, verbose=False, *args, **kwargs):

        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            shell=True
        )
        std_out, std_err = process.communicate()
        if verbose:
            print(std_out.strip(), std_err)
        pass
    def query_filelist_with_date_range_and_area_of_interest(self, id, date, area_of_interest='W-129 N56.2 E-110.4 S31.7', products_id = ['VNP02IMG', 'VNP03IMG'], day_night_modes=['D', 'N'], data_path='data/VNPL1', collection_id='5110'):
        # products_id = ['VNP02IMG', 'VNP03IMG']
        for day_night in day_night_modes:
            for i in range(len(products_id)):
                product_id = products_id[i]
                download_link = self.laads_query_api_link\
                                + '?products='+product_id\
                                + '&collections='+collection_id\
                                + '&archiveSets='+collection_id \
                                + '&temporalRanges=' + date \
                                + '&regions=[BBOX]' + area_of_interest\
                                + '&illuminations=' + day_night
                json_path = os.path.join(data_path, id, date, day_night)
                if os.path.exists(os.path.join(json_path, date + '_' + product_id + '.json')):
                    print('Json already exist, update the Json')
                    os.remove(os.path.join(json_path, date + '_' + product_id + '.json'))

                response = requests.get(download_link, headers=self.header)
                if response.status_code != 200:
                    raise ConnectionRefusedError
                else:
                    if not os.path.exists(json_path):
                        os.makedirs(json_path)

                    with open(os.path.join(json_path, date+'_'+product_id+'.json'), 'wb') as outf:
                        outf.write(response.content)
                    print('New ' + product_id +' file list for day '+date+' created '+day_night)

    def download_files_to_local_based_on_filelist(self, id, date, products_id = ['VNP02IMG', 'VNP03IMG'], day_night_modes=['D', 'N'], json_path='data/VNPL1', data_path='/Volumes/yussd/viirs/VNPNC', collection_id='5110'):
        date_ndays = (datetime.datetime.strptime(date, '%Y-%m-%d')-datetime.datetime.strptime(date[:4]+'-01-01', '%Y-%m-%d')).days+1
        for i in range(len(products_id)):
            for day_night in day_night_modes:
                product_id = products_id[i]
                vnp_json = open(os.path.join(json_path, id, date, day_night, date + '_' + product_id + '.json'), 'rb')
                vnp_list = json.load(vnp_json)['content']
                vnp_list = [file for file in vnp_list if file['archiveSets']==int(collection_id)]
                # print('There are ' + str(vnp_list.__len__()) + ' ' + product_id +' files to download in total.')
                print('Product ID: {}, Day Night : {}'.format(product_id, day_night))
                for vnp_file in vnp_list:
                    vnp_name = vnp_file['name']
                    time_captured = vnp_name.split('.')[2]
                    print('VNP Date: {}, VNP Time: {}'.format(date, time_captured))
                    vnp_link = vnp_file['downloadsLink']
                    if os.path.exists(os.path.join(data_path, date, day_night, time_captured, vnp_name)):
                        md5sum = get_checksum(os.path.join(data_path, date, day_night, time_captured, vnp_name))
                        if md5sum == vnp_file['md5sum']:
                            print('VNP Product: {}, Already Completely Downloaded'.format(vnp_name))
                            continue
                        else:
                            print('VNP Product: {}, Incompletely Downloaded, Start Redownloading'.format(vnp_name))
                            os.remove(os.path.join(data_path, date, day_night, time_captured, vnp_name))
                    if not os.path.exists(os.path.join(data_path, date, day_night, time_captured)):
                        os.makedirs(os.path.join(data_path, date , day_night, time_captured))

                    if not os.path.exists(os.path.join(data_path, date, day_night, time_captured, vnp_name)):
                        # print("Downloading netCDF files " + vnp_name.split('.')[1] + vnp_name.split('.')[
                        #     2] + " from Remote server")
                        wget_command_vnp = "wget " + vnp_link +" --header X-Requested-With:XMLHttpRequest" + " --header \"Authorization: Bearer emhhb3l1dGltOmVtaGhiM2wxZEdsdFFHZHRZV2xzTG1OdmJRPT06MTYzMzk0NzU0NTphZmRlYWY2MjE2ODg0MjQ5MTEzNmE3MTE4MzZkOWYxYjg3MWQzNWMz\" -P " + os.path.join(data_path, date, day_night, time_captured)
                        self.runcmd(wget_command_vnp)
                        print('VNP Product: {}, Download Complete'.format(vnp_name))
if __name__ == '__main__':
    date = '2020-08-08'
    laads_client = LaadsClient()
    laads_client.query_filelist_with_date_range_and_area_of_interest(date)
    laads_client.download_files_to_local_based_on_filelist(date)