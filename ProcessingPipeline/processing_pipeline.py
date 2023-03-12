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

    def read_and_projection(self, date, roi, product_id, dir_data='E:/viirs/VNPNC', dir_tiff='E:/viirs/VNPIMGTIF'):
        dir_list = glob.glob(os.path.join(dir_data, date, '*', '*'))
        dir_list.sort()
        for dir_nc in dir_list:
            DN = dir_nc.split('\\')[-2]
            time_captured = dir_nc.split('\\')[-1]
            if len(glob.glob(dir_nc+'\\*.nc'))!=2 and not os.path.exists(os.path.join(dir_tiff, date, DN, time_captured, "VNP"+product_id + date +'-'+ time_captured + ".tif")):
                print('Current download not complete')
                continue
            if product_id == 'MOD':
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
            if not os.path.exists(save_path + '\\' + date):
                os.mkdir(save_path + '\\' + date)
            if os.path.exists(os.path.join(dir_tiff, date, DN, time_captured, "VNP" + product_id + date +'-'+ time_captured + ".tif")):
                print("The GEOTIFF for time " + date +'-'+ time_captured + " has been created!")
                nc_list = glob.glob(os.path.join(dir_data, date, DN, time_captured, "*.nc"))
                if len(nc_list) != 0:
                    [os.remove(nc_file) for nc_file in nc_list]
                continue
            files = find_files_and_readers(base_dir=dir_nc, reader='viirs_l1b')
            scn = Scene(filenames=files)
            scn.load(list_of_bands)
            if product_id == 'MOD':
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
            if product_id == 'MOD':
                dem_list = glob.glob(dir_nc + "\\M[0-9]*_[0-9]*_[0-9]*.tif")
            else:
                dem_list = glob.glob(dir_nc + "\\I[0-9]*_[0-9]*_[0-9]*.tif")
            dem_list.sort()
            dem_list = ' '.join(map(str, dem_list))
            cmd = "gdalbuildvrt -srcnodata 0 -vrtnodata 0 -separate " + dir_nc + "\\VNP"+ product_id + \
                  date + '-' + time_captured + ".vrt " + dem_list
            subprocess.call(cmd.split())
            cmd = "gdal_translate " + dir_nc + "\\VNP"+product_id + \
                  date +'-'+ time_captured + ".vrt " + os.path.join(save_path, date, DN, time_captured) + "\\VNP" + product_id + \
                  date +'-'+ time_captured + ".tif"
            subprocess.call(cmd.split())
            del new_scn
            del scn
    def crop_to_roi(self, date, id, roi, file, dir_subset, utmzone, product_id):
        if not os.path.exists(dir_subset):
            os.mkdir(dir_subset)
        if not os.path.exists(os.path.join(dir_subset, id)):
            os.mkdir(os.path.join(dir_subset, id))
        output_path = os.path.join(dir_subset, id, file.split('\\')[-1])
        if os.path.exists(output_path.replace('VNP'+product_id, 'VNP' +product_id + 'PRO')):
            return
        cmd='gdalwarp '+'-te ' + str(roi[0]) + ' ' + str(roi[1]) + ' ' + str(roi[2]) + ' ' + str(roi[3]) + ' ' + file + ' ' + output_path
        subprocess.call(cmd.split())
        # subprocess.call(('gdalwarp -t_srs EPSG:'+utmzone+' -tr 375 375'+file + ' ' + output_path +' '+output_path.replace('VNP'+product_id, 'VNP'+product_id+'PRO')).split())
        # os.remove(os.path.join(dir_subset, id, file.split('\\')[-1]))

    def processing(self, date, id, roi, utmzone, product_id, dir_data='data\\VNPL1', dir_tif='data\\VNPIMGTIF', dir_subset='data\\cogsubset'):
        print('id:{}, date:{}'.format(id, date))
        self.read_and_projection(date, roi, product_id, dir_data, dir_tif)
        file_list = glob.glob(os.path.join(dir_tif, date, '*', '*', '*.tif'))
        file_list.sort()
        for file in file_list:
            # subprocess.call(('rio cogeo create ' + file + ' ' +file).split())
            self.crop_to_roi(date, id, roi, file, dir_subset, utmzone, product_id)