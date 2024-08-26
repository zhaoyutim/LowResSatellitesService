import ee
import numpy as np
from PIL import Image
import io
import pyproj
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import subprocess
import os
import glob
import sys
import rasterio
from pathlib import Path
import cv2
root_path = str(Path(__file__).resolve().parents[1]) + "/"
sys.path.insert(0,root_path)
import utils
from utils import config

ee.Authenticate()
ee.Initialize(project=utils.config.project_name)

start_date = "2024-08-12"
end_date = "2024-08-13"
roi = [23.8,38.0,24.0,38.3] #[-118.20, 52.70, -117.71, 52.98] #[-122.10, 39.75, -121.45, 40.25]
wgs84 = ""
crsCode = ""

def save_to_png(image_array, output_file):
    rgb_normalized = cv2.normalize(np.clip(image_array, 0, 5000), None, 0, 255, cv2.NORM_MINMAX, cv2.CV_8U)
    print("RBG SHAPE:", rgb_normalized.shape)
    #rgb_normalized = (image_array / image_array.max() * 255).astype(np.uint8)
    img = Image.fromarray(rgb_normalized)
    img.save(output_file)

def get_data(start_date, end_date, roi):
    roi_string = '_'.join(str(x) for x in roi)
    geom = ee.Geometry.Rectangle(roi)

    image = (ee.ImageCollection('COPERNICUS/S2_HARMONIZED')
        .filterBounds(geom)
        .filterDate(start_date, end_date)
        .sort('CLOUDY_PIXEL_PERCENTAGE')
        .first())
    
    image_id = image.getInfo()['id']

    wgs84 = pyproj.CRS('EPSG:4326')
    crsCode = 'EPSG:32635' #'EPSG:32611'
    utm = pyproj.CRS(crsCode)
    project = pyproj.Transformer.from_crs(wgs84, utm, always_xy=True).transform

    min_x, min_y = project(roi[0], roi[1])
    max_x, max_y = project(roi[2], roi[3])

    width = int((max_x - min_x) / 20)
    height = int((max_y - min_y) / 20)
    print(f"Width: {width}, Height: {height}")

    request = {
        'assetId': image_id,
        'fileFormat': 'NUMPY_NDARRAY',
        'bandIds': ['B12', 'B8', 'B4'],
        'grid': {
            'dimensions': {
                'width': width,
                'height': height
            },
            'affineTransform': {
                'scaleX': 20,
                'shearX': 0,
                'translateX': min_x,
                'shearY': 0,
                'scaleY': -20,  # Negative because origin is top-left
                'translateY': max_y
            },
            'crsCode': crsCode,
        },
        #'visualizationOptions': {'ranges': [{'min': 0, 'max': 5000}]},
    }

    image_array = ee.data.getPixels(request)
    im_float32 = np.zeros(image_array.shape + (len(image_array.dtype.names),), dtype=np.float32)
    for i, field in enumerate(image_array.dtype.names):
        im_float32[..., i] = image_array[field].astype(np.float32)

    print("Array shape:", im_float32.shape)
    print("Array type:", type(im_float32))
    print("Array dtype:", im_float32.dtype)

    output_file = os.path.join(root_path,"data","S2","images",roi_string+start_date+'.png')
    save_to_png(im_float32, output_file)
    print(f"Image saved as {output_file}")

def get_data_all(start_date, end_date, roi):
    roi_string = '_'.join(str(x) for x in roi)
    geom = ee.Geometry.Rectangle(roi)

    ic = (ee.ImageCollection('COPERNICUS/S2_SR')
        .filterBounds(geom)
        .filterDate(start_date, end_date)
        .sort('CLOUDY_PIXEL_PERCENTAGE')
        )
    
    images = ic.toList(ic.size())

    for i in range(ic.size().getInfo()):
        image = ee.Image(images.get(i))
        image_id = image.getInfo()['id']

        wgs84 = pyproj.CRS('EPSG:4326')
        crsCode = 'EPSG:32635' #'EPSG:32611'
        utm = pyproj.CRS(crsCode)
        project = pyproj.Transformer.from_crs(wgs84, utm, always_xy=True).transform

        min_x, min_y = project(roi[0], roi[1])
        max_x, max_y = project(roi[2], roi[3])

        width = int((max_x - min_x) / 20)
        height = int((max_y - min_y) / 20)
        print(f"Width: {width}, Height: {height}")

        request = {
            'assetId': image_id,
            'fileFormat': 'NUMPY_NDARRAY',
            'bandIds': ['B12', 'B8', 'B4'],
            'grid': {
                'dimensions': {
                    'width': width,
                    'height': height
                },
                'affineTransform': {
                    'scaleX': 20,
                    'shearX': 0,
                    'translateX': min_x,
                    'shearY': 0,
                    'scaleY': -20,  # Negative because origin is top-left
                    'translateY': max_y
                },
                'crsCode': crsCode,
            },
            #'visualizationOptions': {'ranges': [{'min': 0, 'max': 5000}]},
        }

        image_array = ee.data.getPixels(request)
        im_float32 = np.zeros(image_array.shape + (len(image_array.dtype.names),), dtype=np.float32)
        for i, field in enumerate(image_array.dtype.names):
            im_float32[..., i] = image_array[field].astype(np.float32)

        print("Array shape:", im_float32.shape)
        print("Array type:", type(im_float32))
        print("Array dtype:", im_float32.dtype)

        output_file = os.path.join(root_path,"data","S2","images",image_id.split("/")[-1]+'.png')
        save_to_png(im_float32, output_file)
        print(f"Image saved as {output_file}")


def infer(start_date,end_date,roi):
    roi_string = '_'.join(str(x) for x in roi)

    image_path = "/home/a/a/aadelow/LowResSatellitesService/data/S2/images/"
    output_path = "/home/a/a/aadelow/LowResSatellitesService/data/S2/out/"
    command = f"python3 /home/a/a/aadelow/LowResSatellitesService/utils/predict.py --model /home/e/b/ebrune/super-resolution/Pytorch-UNet/checkpoints/checkpoint_epoch4.pth --input {image_path} -t 0.4 -o {output_path}"
    print(command)
    subprocess.call(command.split())

    files = glob.glob(output_path+"*.png")
    print("Found files:",files)

    for file in files:
        image = np.array(Image.open(file))
        print(image.shape)
            
        transform = rasterio.transform.from_bounds(roi[0], 
                                        roi[1], 
                                        roi[2], 
                                        roi[3],
                                        width=image.shape[0], 
                                        height=image.shape[1])
        metadata = {
            'driver': 'GTiff', 
            'dtype': 'float32', 
            'nodata': 0.0, 
            'width': image.shape[0], 
            'height': image.shape[1],
            'crs': rasterio.crs.CRS.from_epsg(4326),
            "count": 1,
            "transform": transform
        }
        with rasterio.Env():
            with rasterio.open(output_path+roi_string+start_date+".tif", 'w', **metadata) as dst:
                dst.write(image.astype(rasterio.float32),1)


def upload(start_date,end_date, roi, asset_id, dir_tif):
    dates_list = list(np.arange(np.datetime64(start_date), np.datetime64(end_date)))
    roi_string = '_'.join(str(x) for x in roi)

    #for date in dates_list:
    paths = glob.glob(os.path.join(dir_tif, '*.tif'))
    print("Begin uploading files:",paths)
    for path in paths:
        upload_to_gcloud(path)
        upload_to_gee(path,start_date,asset_id=asset_id)


def upload_to_gcloud(file, gs_path='gs://ai4wildfire/VNPPROJ5/'):
    print('Upload to gcloud')

    file_name = file.split('/')[-1]
    file_name = file_name.replace(".","")[:-3] + '.tif'
    id = 's2'
    gs_path += id + '/' + file_name
    upload_cmd = 'gsutil cp ' + file + ' '+gs_path
    print(upload_cmd)
    os.system(upload_cmd)
    print('finish uploading' + file_name)

def upload_to_gee(file, date, gs_path='gs://ai4wildfire/VNPPROJ5/', asset_id='projects/proj5-dataset/assets/proj5_dataset/'):
    print('start uploading to gee')
    file_name = file.split('/')[-1]
    file_name = file_name.replace(".","")[:-3]

    id = '0000'
    gs_path += "s2" + '/' + file_name + '.tif'
    time_start = str(date) + 'T' + '00' + ':' + '00' + ':00'
    cmd = utils.config.ee_path + ' upload image --force --time_start ' + time_start + ' --asset_id='+asset_id + '/' + file_name + ' --pyramiding_policy=sample '+gs_path
    print(cmd)
    subprocess.call(cmd.split())
    print('Uploading in progress for image ' + time_start)

dag = DAG(
    f'S2_Inference_dag',
    default_args=config.default_args,
    schedule_interval='0 11 * * *',
    description='A DAG for converting {source} Active Fire to progression in gee'
)
with dag:
    convert_task = PythonOperator(
        task_id=f'get_data_task',
        python_callable=get_data,
        op_kwargs={
            'start_date': start_date,
            'end_date': end_date,
            'roi': roi
            }
        )
    
    infer_task = PythonOperator(
        task_id=f'infer_task',
        python_callable=infer,
        op_kwargs={
            'start_date': start_date,
            'end_date': end_date,
            'roi': roi
            }
        )
    
    upload_task = PythonOperator(
        task_id=f'upload_task',
        python_callable=upload,
        op_kwargs={
            'start_date': start_date,
            'end_date': end_date,
            'roi': roi,
            'asset_id': "projects/ee-eo4wildfire/assets/S2UNET",
            'dir_tif':root_path+"data/S2/out"
            }
        )
    convert_task >> infer_task >> upload_task