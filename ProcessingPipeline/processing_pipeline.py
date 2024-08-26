import glob
import os
import subprocess
from pathlib import Path
root_path = str(Path(__file__).resolve().parents[1]) + "/"
import numpy as np
from pyresample import create_area_def
from satpy import find_files_and_readers
from satpy.scene import Scene
import json
from osgeo import gdal
import dask.array as da
import xarray as xr

#https://stackoverflow.com/questions/62439753/how-to-compile-and-use-gdal-with-gpu-supportopencl

class Pipeline:
    def __init__(self):
        return

    def read_and_projection(self, dir_nc, date, product_id, bands, dir_tif, dir_chan):
        time_captured = dir_nc.split('.')[-1][-4:]
        files = find_files_and_readers(base_dir=dir_nc, reader='viirs_l1b')
        files['viirs_l1b'] = [file for file in files['viirs_l1b'] if product_id in file]
        scn = Scene(filenames=files)
        scn.load(bands)
        
        lon_band = 'm_lon' if product_id == 'MOD' else 'i_lon'
        lat_band = 'm_lat' if product_id == 'MOD' else 'i_lat'

        lon = scn[lon_band].values
        lat = scn[lat_band].values

        area = create_area_def(area_id="area", projection='WGS84', shape=(lat.shape[1], lat.shape[0]), lon=lon, lat=lat)
        new_scn = scn.resample(destination=area)

        for n_chan in range(len(bands[:-2])):
            print("n_chan:", n_chan)
            new_scn.save_dataset(
                writer='geotiff', dtype=np.float32, enhance=False,
                filename='{name}_{start_time:%Y%m%d_%H%M%S}.tif',
                dataset_id=bands[n_chan],
                base_dir=dir_nc,
                BIGTIFF='YES')

        tif_files = glob.glob(dir_chan)
        tif_files.sort()

        tif_files_string = ' '.join(map(str, tif_files))

        cmd = "gdalbuildvrt -srcnodata 0 -vrtnodata 0 -separate " + dir_nc + "/VNP"+ product_id + \
                date + '-' + time_captured + ".vrt " + tif_files_string
        print(cmd)
        subprocess.call(cmd.split())
        cmd = "gdal_translate " + dir_nc + "/VNP"+product_id + date +'-'+ time_captured + ".vrt " + dir_tif
        print(cmd)
        subprocess.call(cmd.split())
        
        for tif_file in tif_files:
            os.remove(tif_file)
        del new_scn
        del scn
    
    def crop_to_roi(self, roi, file, output_path):
        print("Cropping image ", file)
        cmd='gdalwarp '+'-te ' + str(roi[0]) + ' ' + str(roi[1]) + ' ' + str(roi[2]) + ' ' + str(roi[3]) + ' ' + file + ' ' + output_path
        print(cmd)
        subprocess.call(cmd.split())
        print("Completed crop. Saved file at ", output_path)
        
    def processing(self, dir_nc, date, roi, product_id, bands, dir_tif, output_path, dir_chan, skip_project=False):
        print(f'Processiong Date:{date}: {dir_nc} ')
        if not skip_project:
            self.read_and_projection(dir_nc, date, product_id, bands, dir_tif, dir_chan)
        self.crop_to_roi(roi, dir_tif, output_path)