from satpy.scene import Scene
from glob import glob
from satpy import find_files_and_readers
from satpy import available_readers
from satpy.composites import GenericCompositor
from satpy.writers import to_image
from pyresample import create_area_def
import matplotlib.pyplot as plt
if __name__=='__main__':
    print(available_readers())
    path_to_data = '/Users/zhaoyu/PycharmProjects/LowResSatellitesService/data/VNPSDR'
    files = find_files_and_readers(base_dir=path_to_data, reader='viirs_sdr')
    scn = Scene(filenames=files)
    print(scn.available_dataset_names())
    print(scn.available_composite_names())
    scn.load(['I01', 'I02', 'I03', 'I04', 'I05'])

    my_area = create_area_def('my_area', projection='epsg:4326', height=scn['I04'].y.size, width=scn['I04'].x.size)
    # compositor = GenericCompositor("overview")
    # composite = compositor([scn['I01'],scn['I02'],scn['I03'],scn['I04'],scn['I05']])
    # # lon=scn['i_lon'].values
    # lat=scn['i_lat'].values
    # area = create_area_def(area_id="area", projection='WGS84', lon=scn['i_lon'].values, lat=scn['i_lat'].values)
    new_scn = scn.resample(destination=my_area)
    # plt.figure()
    # new_scn['I04'].plot.imshow()
    # plt.show()
    new_scn.save_datasets()