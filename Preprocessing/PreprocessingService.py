import datetime
import os
from glob import glob
import matplotlib.pyplot as plt
import cv2
import numpy as np
import rasterio
import yaml

class PreprocessingService:

    def get_composition(self, input_arr, vis_params):
        if len(vis_params) == 1:
            return input_arr
        else:
            composition = np.zeros((3, input_arr.shape[1], input_arr.shape[2]))
            red = vis_params.get("red_band")
            green = vis_params.get("green_band")
            blue = vis_params.get("blue_band")
            composition[0, :, :] = (input_arr[red, :, :]-input_arr[red, :, :].min())/(input_arr[red, :, :].max()-input_arr[red, :, :].min())
            composition[1, :, :] = (input_arr[green, :, :]-input_arr[green, :, :].min())/(input_arr[green, :, :].max()-input_arr[green, :, :].min())
            composition[2, :, :] = (input_arr[blue, :, :]-input_arr[blue, :, :].min())/(input_arr[blue, :, :].max()-input_arr[blue, :, :].min())
            return composition

    def padding(self, coarse_arr, array_to_be_downsampled):
        array_to_be_downsampled = np.pad(array_to_be_downsampled, ((0, 0), (0, coarse_arr.shape[1] * 2 - array_to_be_downsampled.shape[1]), (0, coarse_arr.shape[2] * 2 - array_to_be_downsampled.shape[2])), 'constant', constant_values = (0, 0))
        return array_to_be_downsampled

    def down_sampling(self, input_arr):
        return np.mean(input_arr)

    def read_tiff(self, file_path):
        with rasterio.open(file_path, 'r') as reader:
            profile = reader.profile
            tif_as_array = reader.read()
        return tif_as_array, profile

    def write_tiff(self, file_path, arr, profile):
        with rasterio.Env():
            with rasterio.open(file_path, 'w', **profile) as dst:
                dst.write(arr)

    def get_thresholded_image(self, location):
        path = location + '/S3_Custom/*AF.tif'
        files = glob(path)
        files.sort()
        for file in files:
            time = file[18:29]
            array, profile = self.read_tiff(file)
            array = array.astype(np.float32)
            array[np.where(array <= 140)] = np.nan
            profile.data['dtype']='float32'
            plt.imshow(array[0,:,:])
            plt.show()
            write_path = location+'/S3_th/'
            if not os.path.exists(write_path):
                os.mkdir(write_path)
            self.write_tiff(write_path+time+'_AF.tif', array, profile)

    def get_burn_area_image(self, location):
        path = location + '/S3/*S6.tif'
        files = glob(path)
        files.sort()
        for file in files:
            time = file[11:22]
            array, profile = self.read_tiff(file)
            array = array.astype(np.float32)
            array[np.where(array <= 340)] = np.nan
            profile.data['dtype']='float32'
            plt.imshow(array[0,:,:])
            plt.show()
            write_path = location+'/S3_th/'
            if not os.path.exists(write_path):
                os.mkdir(write_path)
            self.write_tiff(write_path+time+'_F1.tif', array, profile)