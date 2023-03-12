import glob
import logging
import os
import time
from logging.handlers import RotatingFileHandler

logFile = 'log/sanity_check_' + time.strftime("%Y%m%d-%H%M%S") + '.log'

logger = logging.getLogger('my_logger')
handler = RotatingFileHandler(logFile, mode='a', maxBytes=50 * 1024 * 1024,
                              backupCount=5, encoding=None, delay=False)
logger.setLevel(logging.DEBUG)
logger.addHandler(handler)

def sanity_check(dir_nc='E:\\viirs\\VNPNC', dir_tiff='G:\\viirs\\VNPIMGTIF'):
    nc_list = glob.glob(os.path.join(dir_nc, '*', '*', '*'))
    nc_list.sort()
    for nc_file in nc_list:
        date = nc_file.split('\\')[-3]
        DN = nc_file.split('\\')[-2]
        capture_time = nc_file.split('\\')[-1]
        tif_file = os.path.join(dir_tiff, date, DN, capture_time)
        nc_list_of_day =  glob.glob(os.path.join(nc_file, '*.nc'))
        intermediate_tif_list_of_day = glob.glob(os.path.join(nc_file, '*.tif'))
        intermediate_vrt_list_of_day = glob.glob(os.path.join(nc_file, '*.vrt'))
        tif_list_of_day = glob.glob(os.path.join(tif_file, '*.tif'))
        if len(nc_list_of_day) != 0 and len(tif_list_of_day) != 0:
            print('NC file of date {}, day_night {}, capture time {} is not needed please remove'.format(date, DN, capture_time))
            [os.remove(nc_file_of_day) for nc_file_of_day in nc_list_of_day]
            [os.remove(intermediate_tif_file_of_day) for intermediate_tif_file_of_day in intermediate_tif_list_of_day]
            [os.remove(intermediate_vrt_file_of_day) for intermediate_vrt_file_of_day in intermediate_vrt_list_of_day]
        elif len(nc_list_of_day) != 0 and len(tif_list_of_day) == 0:
            logger.info('NC file of date {}, day_night {}, capture time {} is not processed'.format(date, DN, capture_time))
        elif len(nc_list_of_day) == 0 and len(tif_list_of_day) != 0:
            print('TIF file of date {}, day_night {}, capture time {} is correctly created'.format(date, DN, capture_time))
            [os.remove(intermediate_tif_file_of_day) for intermediate_tif_file_of_day in intermediate_tif_list_of_day]
            [os.remove(intermediate_vrt_file_of_day) for intermediate_vrt_file_of_day in intermediate_vrt_list_of_day]
        else:
            if os.path.exists(tif_file):
                os.rmdir(tif_file)
            if os.path.exists(nc_file):
                os.rmdir(nc_file)
            print('date {}, day_night {}, capture time {} does not have any NC files or TIF files'.format(date, DN, capture_time))

if __name__=='__main__':
    sanity_check(dir_nc='G:\\viirs\\VNPNC', dir_tiff='E:\\viirs\\VNPIMGTIF')

