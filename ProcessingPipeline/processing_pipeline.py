import glob
import os
import subprocess

import numpy as np
from pyresample import create_area_def
from satpy import find_files_and_readers
from satpy.scene import Scene


class Pipeline:
    def __init__(self):
        return

    def read_and_projection(self, date, roi, dir_data='/Volumes/yussd/viirs/VNPNC', dir_tiff='/Volumes/yussd/viirs/VNPIMGTIF'):
        dir_list = glob.glob(os.path.join(dir_data, date, '*', '*'))
        dir_list.sort()
        for dir_nc in dir_list:
            DN = dir_nc.split('/')[-2]
            if len(glob.glob(dir_nc+'/*.nc'))!=2:
                print('Current download not complete')
                continue
            time_captured = dir_nc.split('/')[-1]
            if 'MOD' in dir_data:
                save_path = dir_tiff.replace('VNPIMGTIF', 'VNPMODTIF')
                list_of_bands = ['M11', 'm_lat', 'm_lon']
            else:
                save_path = dir_tiff
                if DN == 'D':
                    list_of_bands = ['I01', 'I02', 'I03', 'I04', 'I05', 'i_lat', 'i_lon']
                else:
                    list_of_bands = ['I04', 'I05', 'i_lat', 'i_lon']
            if not os.path.exists(save_path):
                os.mkdir(save_path)
            if not os.path.exists(save_path + '/' + date):
                os.mkdir(save_path + '/' + date)
            print(time_captured)
            if os.path.exists(os.path.join(dir_tiff, date, DN, time_captured, "VNPIMG" + date +'-'+ time_captured + ".tif")):
                print("The GEOTIFF for time " + date +'-'+ time_captured + " has been created!")
                continue
            files = find_files_and_readers(base_dir=dir_nc, reader='viirs_l1b')
            scn = Scene(filenames=files)
            print(scn.available_dataset_ids())
            scn.load(list_of_bands)
            if 'MOD' in save_path:
                lon = scn['m_lon'].values
                lat = scn['m_lat'].values
            else:
                lon = scn['i_lon'].values
                lat = scn['i_lat'].values
            area = create_area_def(area_id="area", projection='WGS84', shape=(lat.shape[1], lat.shape[0]), lon=lon, lat=lat)
            new_scn = scn.resample(destination=area)

            # scene_llbox = new_scn.crop(xy_bbox=roi)
            if not os.path.exists(os.path.join(save_path, date, DN)):
                os.mkdir(os.path.join(save_path, date, DN))
            if not os.path.exists(os.path.join(save_path, date, DN, time_captured)):
                os.mkdir(os.path.join(save_path, date, DN, time_captured))
            # Memory Efficiancy
            for n_chan in range(len(list_of_bands[:-2])):
                new_scn.save_dataset(
                    writer='geotiff', dtype=np.float32, enhance=False,
                    filename='{name}_{start_time:%Y%m%d_%H%M%S}.tif',
                    dataset_id=list_of_bands[n_chan],
                    base_dir=dir_nc)

            dem_list = glob.glob(dir_nc + "/I[0-9]*_[0-9]*_[0-9]*.tif")
            dem_list.sort()
            dem_list = ' '.join(map(str, dem_list))

            if 'MOD' in save_path:
                cmd = "gdalbuildvrt -srcnodata 0 -vrtnodata 0 -separate " + dir_nc + "/VNPMOD" + \
                      date + '-' + time_captured + ".vrt " + dem_list
                subprocess.call(cmd.split())
                cmd = "gdal_translate " + dir_nc + "/VNPMOD" + \
                      date +'-'+ time_captured + ".vrt " + os.path.join(save_path, date, DN, time_captured) + "/VNPMOD" + \
                      date +'-'+ time_captured + ".tif"
                subprocess.call(cmd.split())
            else:
                cmd = "gdalbuildvrt -srcnodata 0 -vrtnodata 0 -separate " + dir_nc + "/VNPIMG" + \
                      date + '-' + time_captured + ".vrt " + dem_list
                subprocess.call(cmd.split())
                cmd = "gdal_translate " + dir_nc + "/VNPIMG" + \
                      date + '-' + time_captured + ".vrt " + os.path.join(save_path, date, DN, time_captured) + "/VNPIMG" + \
                      date + '-' + time_captured + ".tif"
                subprocess.call(cmd.split())
            del new_scn
            del scn
    def crop_to_roi(self, date, id, roi, file, dir_subset, utmzone):
        if not os.path.exists(dir_subset):
            os.mkdir(dir_subset)
        if not os.path.exists(os.path.join(dir_subset, id)):
            os.mkdir(os.path.join(dir_subset, id))
        output_path = os.path.join(dir_subset, id, file.split('/')[-1])
        if os.path.exists(output_path.replace('VNPIMG', 'VNPIMGPRO')):
            return
        cmd='gdalwarp '+'-te ' + str(roi[0]) + ' ' + str(roi[1]) + ' ' + str(roi[2]) + ' ' + str(roi[3]) + ' ' + file + ' ' + output_path
        print(cmd)
        subprocess.call(cmd.split())
        subprocess.call(('gdalwarp -t_srs EPSG:'+utmzone+' -tr 375 375'+file + ' ' + output_path +' '+output_path.replace('VNPIMG', 'VNPIMGPRO')).split())
        os.remove(os.path.join(dir_subset, id, file.split('/')[-1]))

    def processing(self, date, id, roi, utmzone, dir_data='data/VNPL1', dir_tif='data/VNPIMGTIF', dir_subset='data/cogsubset'):
        self.read_and_projection(date, roi, dir_data)
        file_list = glob.glob(os.path.join(dir_tif, date, '*', '*', '*.tif'))
        file_list.sort()
        for file in file_list:
            # subprocess.call(('rio cogeo create ' + file + ' ' +file).split())
            self.crop_to_roi(date, id, roi, file, dir_subset, utmzone)