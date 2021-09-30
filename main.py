from satpy.scene import Scene
from glob import glob
from satpy import find_files_and_readers
from satpy import available_readers
from satpy.composites import GenericCompositor
from satpy.writers import to_image
from pyresample import create_area_def
import matplotlib.pyplot as plt
from pyproj import Transformer
if __name__=='__main__':
    print(available_readers())
    path_to_data = '/Users/zhaoyu/PycharmProjects/LowResSatellitesService/data/VNP02IMG/2510000'
    files = find_files_and_readers(base_dir=path_to_data, reader='viirs_l1b')
    scn = Scene(filenames=files)
    print(scn.available_dataset_names())
    print(scn.available_composite_names())
    scn.load(['I01','I02','I03','I04','I05', 'i_lat', 'i_lon'])

    # my_area = create_area_def('my_area', projection='epsg:32610', resolution=scn['I04'].resolution, area_extent=[bot_left[0], bot_left[1], top_right[0], top_right[1]])
    #
    # new_scn = scn.resample(destination=my_area)
    # compositor = GenericCompositor("overview")
    # composite = compositor([scn['I01'],scn['I02'],scn['I03'],scn['I04'],scn['I05']])
    lon=scn['i_lon'].values
    lat=scn['i_lat'].values
    area = create_area_def(area_id="area", projection='WGS84', shape=(lat.shape[1],lat.shape[0]), lon=lon, lat=lat)
    new_scn = scn.resample(destination=area)
    new_scn.save_datasets(
        filename='{name}_{start_time:%Y%m%d_%H%M%S}.tif',
        datasets=['I01','I02','I03','I04','I05'],
        base_dir=path_to_data)
    # # scn_crop =
    # plt.figure()
    # new_scn['I03'].plot.imshow()
    # plt.show()
    # new_scn.save_datasets()