# -*- coding: utf-8 -*-
import argparse
import datetime
import glob
import multiprocessing
import os
import platform
import sys

import pandas as pd

from LaadsDataHandler.laads_client import LaadsClient


def json_wrapper(args):
    os.dup2(sys.stdout.fileno(), 1)
    return laads_client.query_filelist_with_date_range_and_area_of_interest(*args)


def client_wrapper(args):
    os.dup2(sys.stdout.fileno(), 1)
    return laads_client.download_files_to_local_based_on_filelist(*args)


def get_json_tasks(start_date, duration, area_of_interest, products_id, day_night, dir_json, collection_id):
    tasks = []
    for k in range(duration.days):
        tasks.append(
            (
                id,
                (datetime.datetime.strptime(start_date, '%Y-%m-%d') + datetime.timedelta(k)).strftime(
                    '%Y-%m-%d'),
                area_of_interest, products_id, ['D'], dir_json, collection_id
            )
        )
    return tasks


def get_client_tasks(id, start_date, duration, products_id, day_night, dir_json, dir_nc, collection_id):
    tasks = []
    for k in range(duration.days):
        tasks.append(
            (
                id,
                (datetime.datetime.strptime(start_date, '%Y-%m-%d') + datetime.timedelta(k)).strftime(
                    '%Y-%m-%d'),
                products_id, ['D'], dir_json, dir_nc, collection_id
            )
        )
    return tasks

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('-pid', type=str, help='Product ID IMG or MOD')
    parser.add_argument('-mode', type=str, help='Mode to be selected between roi based or csv based')
    parser.add_argument('-y', type=str, help='Year if mode is selected as csv')
    parser.add_argument('-roi', type=str, help='lon_min,lat_min,lon_max,lat_max')
    parser.add_argument('-sd', type=str, help='Start Date if mode is selected as roi')
    parser.add_argument('-ed', type=str, help='Start Date if mode is selected as roi')
    parser.add_argument('-rs', type=bool, help='Resume downloading')
    args = parser.parse_args()
    mode = args.mode
    product_id = args.pid
    rs = args.rs
    dates = ['2018-07-03', '2018-07-04', '2018-07-05', '2018-07-06', '2018-07-07']
    if platform.system()=='Windows':
        dir_json = 'data\\VNPL1'
        dir_nc = 'G:\\viirs\\VNPNC'
    elif platform.system()=='Darwin':
        dir_json = 'data\\VNPL1'
        dir_nc = 'G:\\viirs\\VNPNC'
    else:
        dir_json = 'data/VNPL1'
        dir_nc = 'data/VNPNC'
    num_processes = 16
    laads_client = LaadsClient()
    collection_id = '5200' # 5110 for VNP series
    products_id = ['VNP02'+product_id, 'VNP03'+product_id] #['VJ102IMG', 'VJ103IMG'] ['VNP02MOD', 'VNP03MOD'], ['VNP02IMG', 'VNP03IMG'], ['VJ102MOD', 'VJ103MOD']

    if mode == 'csv':
        year = args.y
        filename = 'roi/us_fire_' + year + '_out_new.csv'
        df = pd.read_csv(filename)
        df = df.sort_values(by=['Id'])
        ids, start_dates, end_dates, lons, lats = df['Id'].values.astype(str), df['start_date'].values.astype(str), df['end_date'].values.astype(str), df['lon'].values.astype(float), df['lat'].values.astype(float)
        for i, id in enumerate(ids):
            if rs:
                log_list = glob.glob('log/'+'nc_check*.log')
                log_list.sort()
                log_file = log_list[-1]
                with open(log_file) as f:
                    file = f.read()
                    missing_list = file.split('\n')
                    missing_list = missing_list[:-1]
                    missing_dates = [i.split('/')[3] for i in missing_list]
            lon, lat, start_date, end_date = lons[i], lats[i], start_dates[i], end_dates[i]
            roi_size = 1
            roi = [lon - roi_size, lat - roi_size, lon + roi_size, lat + roi_size]
            duration = datetime.datetime.strptime(end_date, '%Y-%m-%d')-datetime.datetime.strptime(start_date, '%Y-%m-%d')
            area_of_interest = 'W'+str(roi[0])+' '+'N'+str(roi[3])+' '+'E'+str(roi[2])+' '+'S'+str(roi[1])
            print('Currently processing id {}'.format(id))
            tasks = get_json_tasks(start_date, duration, area_of_interest, products_id, ['D'], dir_json, collection_id)
            tasks2 = get_client_tasks(id, start_date, duration, products_id, ['D'], dir_json, dir_nc, collection_id)

    elif mode == 'roi':
        roi_arg = args.roi
        start_date = args.sd
        end_date = args.ed
        roi = [float(roi_arg.split(',')[0]), float(roi_arg.split(',')[1]), float(roi_arg.split(',')[2]), float(roi_arg.split(',')[3])]
        duration = datetime.datetime.strptime(end_date, '%Y-%m-%d') - datetime.datetime.strptime(start_date, '%Y-%m-%d')
        area_of_interest = 'W' + str(roi[0]) + ' ' + 'N' + str(roi[3]) + ' ' + 'E' + str(roi[2]) + ' ' + 'S' + str(
            roi[1])
        id = 'ASIA'
        print('Currently processing id {}'.format(id))
        tasks = get_json_tasks(start_date, duration, area_of_interest, products_id, ['D'], dir_json, collection_id)
        tasks2 = get_client_tasks(id, start_date, duration, products_id, ['D'], dir_json, dir_nc, collection_id)
    else:
        raise('No Support Mode')
    with multiprocessing.Pool(processes=4) as pool:
        results = list(pool.imap_unordered(json_wrapper, tasks))
    print(results)
    with multiprocessing.Pool(processes=4) as pool:
        results = list(pool.imap_unordered(client_wrapper, tasks2))
    print(results)
