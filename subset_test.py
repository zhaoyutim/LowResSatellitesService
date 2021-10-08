from glob import glob

from pyresample import create_area_def
from satpy import find_files_and_readers, Scene
import numpy as np

path = 'data/VNPL1/191/2006/'

if __name__ == '__main__':
    files = find_files_and_readers(base_dir=path, reader='viirs_l1b')
    scn = Scene(filenames=files)
    scn.load(['I01','I02','I03','I04','I05', 'i_lat', 'i_lon'])

    lon=scn['i_lon'].values
    lat=scn['i_lat'].values
    area = create_area_def(area_id="area", projection='EPSG:4326', shape=(lat.shape[1],lat.shape[0]), lon=lon, lat=lat)

    new_scn = scn.resample(destination=area)
    scene_llbox = new_scn.crop(xy_bbox=(-124.5, 32.44, -114, 41.68))

    scene_llbox.save_datasets(
        writer='geotiff', dtype=np.float32, enhance=False,
        filename='{name}_{start_time:%Y%m%d_%H%M%S}.tif',
        datasets=['I01','I02','I03','I04','I05'],
        base_dir=path)


