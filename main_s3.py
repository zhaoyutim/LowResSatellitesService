import oauth as oauth
from satpy.scene import Scene
from glob import glob
from satpy import find_files_and_readers
from satpy import available_readers
from satpy.composites import GenericCompositor
from satpy.writers import to_image
from pyresample import create_area_def
import matplotlib.pyplot as plt
from oauthlib.oauth2 import BackendApplicationClient
from requests_oauthlib import OAuth2Session
from sentinelhub import SHConfig
from sentinelhub import MimeType, CRS, BBox, SentinelHubRequest, SentinelHubDownloadClient, \
    DataCollection, bbox_to_dimensions, DownloadRequest

if __name__=='__main__':
    # Your client credentials
    client_id = '4456b422-fe16-4f5c-90b1-5a9583020e10'
    client_secret = '@(KXCxP34*e:nFh@/azmkH~*cEnA%KwT-qm{!q!-'
    # Create a session
    # client = BackendApplicationClient(client_id=client_id)
    # oauth = OAuth2Session(client=client)
    # # Get token for the session
    # token = oauth.fetch_token(token_url='https://services.sentinel-hub.com/oauth/token',
    #                           client_id=client_id, client_secret=client_secret)
    # # All requests using this session will have an access token automatically added
    # resp = oauth.get("https://services.sentinel-hub.com/oauth/tokeninfo")
    # print(resp.content)
    # print(token)
    config = SHConfig()

    if config.sh_client_id == '' or config.sh_client_secret == '':
        print("Warning! To use Sentinel Hub Process API, please provide the credentials (client ID and client secret).")
    else:
        print('Using Id:{}'.format(config.sh_client_id))

    betsiboka_coords_wgs84 = [-118.8, 36.7, -119.8, 37.7]

    resolution = 1000
    betsiboka_bbox = BBox(bbox=betsiboka_coords_wgs84, crs=CRS.WGS84)
    betsiboka_size = bbox_to_dimensions(betsiboka_bbox, resolution=resolution)

    print(f'Image shape at {resolution} m resolution: {betsiboka_size} pixels')

    evalscript_true_color = """
        //VERSION=3

        function setup() {
            return {
                input: [{
                    bands: ["S7"]
                }],
                output: {
                    bands: 1
                }
            };
        }

        function evaluatePixel(sample) {
            return [0.002*sample.S7];
        }
    """

    request_true_color = SentinelHubRequest(
        evalscript=evalscript_true_color,
        input_data=[
            SentinelHubRequest.input_data(
                data_collection=DataCollection.SENTINEL3_SLSTR,
                time_interval=('2020-09-07', '2020-09-08'),
            )
        ],
        responses=[
            SentinelHubRequest.output_response('default', MimeType.TIFF)
        ],
        bbox=betsiboka_bbox,
        size=betsiboka_size,
        config=config
    )
    true_color_imgs = request_true_color.get_data()
    image = true_color_imgs[0]
    print(f'Image type: {image.dtype}')

    # plot function
    # factor 1/255 to scale between 0-1
    # factor 3.5 to increase brightness
    plt.imshow(image, vmin=image.min(), vmax=image.max(), cmap='Greys')
    plt.show()