import billiard as multiprocessing
import datetime
import os
import main_mosaic
from pathlib import Path
import sys
import glob
import numpy as np
root_path = str(Path(__file__).resolve().parents[1]) + "/"
sys.path.insert(0,root_path)
from utils.utils import *

def patch_image(paths, save_path):
    path = paths[0]
    tif, _ = main_mosaic.read_tiff(path)
    image = np.array(tif)
    patched_image = patch_image(image)

    for path in paths[1:]:
        tif, _ = main_mosaic.read_tiff(path)
        image = np.array(tif)
        patched_image = np.concatenate((patched_image, patch_image(image)),axis=2)
    np.save(save_path, patched_image)
    print("Saved batched pathches", save_path)


def patch_region(id,file,roi,date):
    product_id = "IMG" if file else "MOD"
    dn = file.split("/")[-2]
    print("Cropping image ", file)
    output_path = os.path.join(root_path + "data/patched_regions", id, date, dn, str(roi[0]) + '_' + str(roi[1]) + '_' + str(roi[2]) + '_' + str(roi[3]))
    os.makedirs(output_path,exist_ok=True)
    output_path = os.path.join(output_path, file.split('/')[-1])
    if os.path.exists(output_path.replace('VNP'+product_id, 'VNP' +product_id + 'PRO')):
        return
    cmd='gdalwarp '+'-te ' + str(roi[0]) + ' ' + str(roi[1]) + ' ' + str(roi[2]) + ' ' + str(roi[3]) + ' ' + file + ' ' + output_path
    print(cmd)
    subprocess.call(cmd.split())
    print("Completed crop. Saved file at ", output_path)
                

def create_mosaic(id, roi, date, dir_subset):
    roi_string = '_'.join(str(x) for x in roi)
    date_path = os.path.join(root_path,'data','mosaics', id, date)
    os.makedirs(date_path,exist_ok=True)
    save_path = os.path.join(date_path,'VNP'+roi_string+'.tif')
    print('Processing: ' + date)
    print('Looking for files at', os.path.join(dir_subset, id, date, 'D', roi_string, 'VNPIMG'+'*.tif'))
    img_day_tiff_files = glob.glob(os.path.join(dir_subset, id, date, 'D', roi_string, 'VNPIMG'+'*.tif'), recursive=True)
    img_night_tiff_files = glob.glob(os.path.join(dir_subset, id, date, 'N', roi_string, 'VNPIMG'+'*.tif'), recursive=True)
    img_night_tiff_files.extend(glob.glob(os.path.join(dir_subset, id, date, 'B', roi_string, 'VNPIMG'+'*.tif'), recursive=True))
    mod_tiff_files = glob.glob(os.path.join(dir_subset, id, date, 'D', roi_string, 'VNPMOD*.tif'), recursive=True)
    tiff_files =[img_day_tiff_files,mod_tiff_files,img_night_tiff_files]
    channel_names=['D','MOD','BN']
    print('Found ', len(img_day_tiff_files)+len(img_night_tiff_files)+len(mod_tiff_files), ' files')
    print('Remove Nan')
    combine_paths = []

    for channel in range(3):
        for file in tiff_files[channel]:
            print("Reading file: ", file)
            array, profile = main_mosaic.read_tiff(file)
            array = np.nan_to_num(array)
            main_mosaic.write_tiff(file, array, profile)
        print('Finish remove Nan')

        if len(tiff_files[channel]) !=0:
            mosaic, mosaic_metadata = main_mosaic.mosaic_geotiffs(tiff_files[channel])
            os.makedirs(os.path.join(root_path+'data/mosaics/channels',id,date,channel_names[channel]),exist_ok=True)
            output_path = os.path.join(root_path+'data/mosaics/channels',id,date, channel_names[channel], 'VNP'+roi_string+'.tif')
            main_mosaic.write_tiff(output_path, mosaic, mosaic_metadata)
            combine_paths.append(output_path)
            print("Created mosaic for ", channel_names[channel])
    
    main_mosaic.combine_tiff(combine_paths, save_path)
    print('Finish Creating mosaic ', save_path)


def download_viirs(id, roi_arg, start_date, end_date, dir_json, dir_nc, dn_img = [], dn_mod = [], collection_id = '5200',products_id_img = ['VNP02IMG', 'VNP03IMG'], products_id_mod = ['VNP02MOD','VNP03MOD']):
    print("Downloading files from", start_date,"to",end_date)
    roi = [float(roi_arg.split(',')[0]), float(roi_arg.split(',')[1]), float(roi_arg.split(',')[2]),
           float(roi_arg.split(',')[3])]
    duration = datetime.datetime.strptime(end_date, '%Y-%m-%d') - datetime.datetime.strptime(start_date, '%Y-%m-%d')
    area_of_interest = 'W' + str(roi[0]) + ' ' + 'N' + str(roi[3]) + ' ' + 'E' + str(roi[2]) + ' ' + 'S' + str(roi[1])

    json_tasks,client_tasks = [],[]
    if len(dn_img) != 0:
        json_tasks.extend(get_json_tasks(id, start_date, duration, area_of_interest, products_id_img, dn_img, dir_json, collection_id))
        client_tasks.extend(get_client_tasks(id, start_date, duration, products_id_img, dn_img, dir_json, dir_nc, collection_id))

    if len(dn_mod) != 0:
        json_tasks.extend(get_json_tasks(id, start_date, duration, area_of_interest, products_id_mod, dn_mod, dir_json, collection_id))
        client_tasks.extend(get_client_tasks(id, start_date, duration, products_id_mod, dn_mod, dir_json, dir_nc, collection_id))

    with multiprocessing.Pool(processes=8) as pool:
        list(pool.imap_unordered(json_wrapper, json_tasks))

    with multiprocessing.Pool(processes=8) as pool:
        list(pool.imap_unordered(client_wrapper, client_tasks))
    
