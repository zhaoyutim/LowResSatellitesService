import datetime
import os

import yaml
from easydict import EasyDict
from pyproj import CRS
from sentinelhub import SHConfig, SentinelHubRequest, DataCollection, MimeType, BBox, bbox_to_dimensions, CRS, \
    SentinelHubDownloadClient
import matplotlib.pyplot as plt

from LowResSatellitesService.Satellites.MODIS import MODIS
from LowResSatellitesService.Satellites.Sentinel3 import Sentinel3


class LowResSatellitesService:
    def __init__(self):
        with open("LowResSatellitesService/secrets.yaml", "r", encoding="utf8") as f:
            self.secret = yaml.load(f, Loader=yaml.FullLoader)

        # with open("roi/US2020.yaml", "rt") as f:
        #     self.fire_locations = EasyDict(yaml.load(f, Loader=yaml.UnsafeLoader))
        with open("roi/custom_fire.yaml", "rt") as f:
            self.fire_locations = yaml.load(f, Loader=yaml.UnsafeLoader)

        self.config = SHConfig()

        if self.config.sh_client_id == '' or self.config.sh_client_secret == '':
            print(
                "Warning! To use Sentinel Hub Process API, please provide the credentials (client ID and client secret).")
            self.registerate_new_id()
        else:
            print('Using Id:{}'.format(self.config.sh_client_id))

    def get_client_from_satellite_name(self, satellites):
        if satellites == "S3":
            return [Sentinel3("TIR"), Sentinel3("SWIR")]
        elif satellites == "MODIS":
            return [MODIS()]
        else:
            raise NameError("No satellite info provided")

    def get_data_collection_from_satellite_name(self, satellites):
        if satellites == "S3":
            return DataCollection.SENTINEL3_SLSTR
        elif satellites == "MODIS":
            return DataCollection.MODIS


    def registerate_new_id(self):
        self.config.instance_id = self.secret.get('sentinel_hub_instance_id')
        self.config.sh_client_id = self.secret.get('sentinel_hub_client_id')
        self.config.sh_client_secret = self.secret.get('sentinel_hub_client_secret')

    def fetch_imagery_from_sentinel_hub_custom(self, location, satellites='S3'):
        start_date = self.fire_locations.get(location)['start']
        end_date = datetime.date.today()
        timedif = (end_date-start_date).days
        for satellite in satellites:
            requests_list=[]
            for i in range(timedif):
                requests_list.append(self.get_custom_request(location, start_date+datetime.timedelta(i), start_date+datetime.timedelta(i)))

            requests_list = [request.download_list[0] for request in requests_list]
            data = SentinelHubDownloadClient(config=self.config).download(requests_list, max_threads=5)
            for i in range(len(requests_list)):
                # print((start_date+datetime.timedelta(i)).strftime("%Y%m%d"))
                tiff_name = requests_list[i].get_storage_paths()[1]
                os.rename(tiff_name, location+'/'+'S3_Custom'+'/'+(start_date+datetime.timedelta(i)).strftime("%Y%m%d")+'T'+'17'+'_'+satellite+'.band'+'AF'+'.tif')
                os.remove(requests_list[i].get_storage_paths()[0])
                os.removedirs(requests_list[i].get_storage_paths()[0].replace('/request.json', ''))

    def fetch_imagery_from_sentinel_hub(self, location, satellites='S3'):
        # start_date = datetime.datetime.strptime(self.fire_locations.get(location)['start'], '%Y-%m-%d')
        # end_date = datetime.datetime.strptime(self.fire_locations.get(location)['end'], '%Y-%m-%d')
        start_date = self.fire_locations.get(location)['start']
        end_date = self.fire_locations.get(location)['end']
        end_date = datetime.datetime.today()
        timedif = (end_date-start_date).days
        for satellite in satellites:
            clients = self.get_client_from_satellite_name(satellite)

            for client in clients:
                for time in client.times:
                    for band_name in client.band_names:
                        requests_list = [self.get_request_template(location, satellite, start_date+datetime.timedelta(i), start_date+datetime.timedelta(i), time, band_name, client.units, client.resolution, client.pixel_scale) for i in range(timedif)]
                        requests_list = [request.download_list[0] for request in requests_list]
                        data = SentinelHubDownloadClient(config=self.config).download(requests_list, max_threads=5)
                        for i in range(len(requests_list)):
                            # print((start_date+datetime.timedelta(i)).strftime("%Y%m%d"))
                            tiff_name = requests_list[i].get_storage_paths()[1]
                            os.rename(tiff_name, location+'/'+satellite+'/'+(start_date+datetime.timedelta(i)).strftime("%Y%m%d")+'T'+time+'_'+satellite+'.band'+band_name.replace('B', '')+'.tif')
                            os.remove(requests_list[i].get_storage_paths()[0])
                            os.removedirs(requests_list[i].get_storage_paths()[0].replace('/request.json', ''))
    def get_custom_request(self, location, start_time, end_time):
        size = 1
        lon = self.fire_locations.get(location)['lon']
        lat = self.fire_locations.get(location)['lat']
        roi = [lon - size, lat - size, lon + size, lat + size]
        # roi=[-180,-90,180,90]
        boundingbox = BBox(bbox=roi, crs=CRS.WGS84)

        bbox_size = bbox_to_dimensions(boundingbox, resolution=500)
        evalscript_true_color = """
            //VERSION=3
            function setup() {
                return {
                    input: [{
                        bands: ["S6", "S5", "S3", "S2", "S1"]
                    }],
                    output: {
                        bands: 1
                    }
                };
            }
            function evaluatePixel(sample) {
                var SAHM= ((sample.S6 - sample.S5) / (sample.S6 + sample.S5));
                if (SAHM>.05 && sample.S1<.23) {
                    return [5*sample.S3];
                }
                else{
                    return [sample.S6];
                }
            }
        """

        request = SentinelHubRequest(
            evalscript=evalscript_true_color,
            data_folder=location + '/' + 'S3_Custom',
            input_data=[
                SentinelHubRequest.input_data(
                    data_collection=DataCollection.SENTINEL3_SLSTR,
                    time_interval=(
                    start_time.strftime("%Y-%m-%d") + 'T00:00:00Z', end_time.strftime("%Y-%m-%d") + 'T23:59:00Z')
                )
            ],
            responses=[
                SentinelHubRequest.output_response('default', MimeType.TIFF)
            ],
            bbox=boundingbox,
            size=bbox_size,
            config=self.config
        )

        return request


def get_request_template(self, location, satellites, start_time, end_time, time, band_name, units, resolution, pixel_scale):
        size = 10
        lon = self.fire_locations.get(location)['lon']
        lat = self.fire_locations.get(location)['lat']
        roi = [lon-size, lat-size, lon+size, lat+size]
        # roi=[-180,-90,180,90]
        boundingbox = BBox(bbox=roi, crs=CRS.WGS84)

        bbox_size = bbox_to_dimensions(boundingbox, resolution=resolution)
        evalscript_true_color = """
            //VERSION=3
            function setup() {{
                return {{
                    input: [{{
                        bands: ["{}"],
                        units: "{}"
                    }}],
                    output: {{
                        bands: 1,
                        sampleType: "UINT16"
                    }}
                }};
            }}
            function evaluatePixel(sample) {{
                return [{} * sample.{}];
            }}
        """.format(band_name, units, pixel_scale, band_name)
        if satellites == "S3":
            if time == '05':
                start_timestamp = 'T00:00:00Z'
                end_timestamp = 'T12:59:00Z'
            elif time == '17':
                start_timestamp = 'T13:00:00Z'
                end_timestamp = 'T23:59:00Z'
            else:
                raise NotImplementedError("Invalid mode")
        else:
            start_timestamp = 'T00:00:00Z'
            end_timestamp = 'T23:59:00Z'

        request = SentinelHubRequest(
            evalscript=evalscript_true_color,
            data_folder=location + '/' + satellites,
            input_data=[
                SentinelHubRequest.input_data(
                    data_collection=self.get_data_collection_from_satellite_name(satellites),
                    time_interval=(start_time.strftime("%Y-%m-%d")+start_timestamp, end_time.strftime("%Y-%m-%d")+end_timestamp)
                )
            ],
            responses=[
                SentinelHubRequest.output_response('default', MimeType.TIFF)
            ],
            bbox=boundingbox,
            size=bbox_size,
            config=self.config
        )
        # print(f'Image shape at {resolution} m resolution: {bbox_size} pixels')
        return request
