import datetime
import os

from sentinelhub import SentinelHubDownloadClient, BBox, CRS, BBoxSplitter, bbox_to_dimensions, SentinelHubRequest, \
    DataCollection, MimeType, SHConfig


def fetch_imagery_from_sentinel_hub_custom(start_date, end_date, config, location, satellite, roi):
    '''
    :param start_date: 开始时间
    :param end_date: 结束时间
    :param config: sentinelhub configuration
    :param location: 存储目录-地点
    :param satellite: 存储目录-卫星
    :param roi: 自定义roi
    '''
    timedif = (end_date - start_date).days
    requests_list = []
    for i in range(timedif):
        requests_list.append(get_custom_request(roi, location, start_date + datetime.timedelta(i),
                                                     start_date + datetime.timedelta(i), config))
    for i in range(len(requests_list)):
        tile_list = [request.download_list[0] for request in requests_list[i]]
        data = SentinelHubDownloadClient(config=config).download(tile_list, max_threads=5)
        date = (start_date + datetime.timedelta(i)).strftime("%Y%m%d")
        # 整理目录and重命名
        path_date = location + '/' + satellite + '_Custom' + '/' + date
        for j in range(len(tile_list)):
            # print((start_date+datetime.timedelta(i)).strftime("%Y%m%d"))
            tiff_name = tile_list[j].get_storage_paths()[1]

            if not os.path.exists(path_date):
                os.mkdir(path_date)
            os.rename(tiff_name,
                      location + '/' + satellite + '_Custom' + '/' + date + '_' + satellite + '_' + str(j) + '.tif')
            os.remove(tile_list[j].get_storage_paths()[0])
            os.removedirs(tile_list[j].get_storage_paths()[0].replace('/request.json', ''))
            os.rename(location + '/' + satellite + '_Custom' + '/' + date + '_' + satellite + '_' + str(j) + '.tif',
                      location + '/' + satellite + '_Custom' + '/' + date + '/' + date + '_' + satellite + '_' + str(
                          j) + '.tif')


def get_custom_request(roi, location, start_time, end_time, config):
    '''
    :param roi: 自定义roi
    :param location: 存储目录-地点
    :param start_time: 开始时间
    :param end_time: 结束时间
    :param config: sentinelhub configuration
    :return: request template
    '''
    boundingbox = BBox(bbox=roi, crs=CRS.WGS84)
    # 将bbox分割成8，8矩阵，需要根据实际roi大小分割使得每一小块不超过2500
    bbox_splitter = BBoxSplitter(
        [boundingbox.geometry],
        CRS.WGS84,
        (8, 8)
    )
    bbox_list = bbox_splitter.get_bbox_list()
    request=[]
    # 对grid里面每个bbox都生成一个template
    for i in range(len(bbox_list)):
        bbox_size = bbox_to_dimensions(bbox_list[i], resolution=20)

        # template 尽量不要改容易出错，而且很难debug，如果要改band需要改input output和 function 三个地方
        evalscript_true_color = """
            //VERSION=3
            function setup() {
                return {
                    input: [{
                        bands: ["B02","B03","B04","B08","B11","B12"],
                        units: "DN"
                    }],
                    output: {
                        bands: 6,
                        sampleType: "INT16"
                    }
                };
            }
            function evaluatePixel(sample) {
                return [sample.B02,
                        sample.B03,
                        sample.B04,
                        sample.B08,
                        sample.B11,
                        sample.B12];
            }
        """

        request.append(SentinelHubRequest(
            evalscript=evalscript_true_color,
            data_folder=location + '/'+'S2_Custom',
            input_data=[
                SentinelHubRequest.input_data(
                    data_collection=DataCollection.SENTINEL2_L2A,
                    time_interval=(
                    start_time.strftime("%Y-%m-%d") + 'T00:00:00Z', end_time.strftime("%Y-%m-%d") + 'T23:59:00Z')
                )
            ],
            responses=[
                SentinelHubRequest.output_response('default', MimeType.TIFF)
            ],
            bbox=bbox_list[i],
            size=bbox_size,
            config=config
        ))

    return request

if __name__=='__main__':
    # Sentinel hub configuration 不要上传git，这段不能泄漏
    config = SHConfig()
    config.instance_id = 'd859101e-0283-47b0-8030-a8260e0bb995'
    config.sh_client_id = '4bb2314e-4bbd-4f8c-9673-5e08e98d53b8'
    config.sh_client_secret = 'Cj/b3)+1qW96tNL(r.mJ3lLRVzsNv?^I3sLFug{+'
    config.save()

    # bbox中心点和bbox的大小
    lon = -121.47
    lat = 51.00
    size = 1
    # 可以直接设置roi
    roi = [lon - size, lat - size, lon + size, lat + size]
    # 开始结束时间
    start_date = datetime.datetime.strptime('2021-07-26', '%Y-%m-%d')
    end_date = datetime.datetime.strptime('2021-07-29', '%Y-%m-%d')
    # 主函数
    fetch_imagery_from_sentinel_hub_custom(start_date, end_date, config, 'kamloop', 'S2', roi)