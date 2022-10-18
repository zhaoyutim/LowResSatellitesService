from glob import glob

from osgeo import gdal
from pyresample import create_area_def
from satpy import find_files_and_readers, Scene, available_readers
import numpy as np

path = 'data/VNPL1/2020-08-22/0918'

if __name__ == '__main__':
    files=glob(path+'/*.nc')
    for file in files:
        print(gdal.Info(file, format='json')['metadata']['SUBDATASETS'].__len__())
    # print(available_readers())
    # files = find_files_and_readers(base_dir=path, reader='viirs_sdr')
    # scn = Scene(filenames=files)
    # scn.load(['I01','I02','I03','I04','I05', 'i_lat', 'i_lon'])
    # pass
    # lon=scn['i_lon'].values
    # lat=scn['i_lat'].values
    # area = create_area_def(area_id="area", projection='EPSG:4326', shape=(lat.shape[1],lat.shape[0]), lon=lon, lat=lat)

    # new_scn = scn.resample(destination=area)
    # scene_llbox = scn.crop(xy_bbox=(13.38,61.55,15.59,62.07))
    # if not np.isnan(scene_llbox['I04'].values).all():
    #     scene_llbox.save_datasets(
    #         writer='geotiff', dtype=np.float32, enhance=False,
    #         filename='{name}_{start_time:%Y%m%d_%H%M%S}.tif',
    #         datasets=['I01','I02','I03','I04','I05'],
    #         base_dir=path)


