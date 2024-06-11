# -*- coding: utf-8 -*-
import argparse
import datetime
import glob
import multiprocessing
import platform

import pandas as pd

from utils.utils import get_json_tasks, get_client_tasks, json_wrapper, client_wrapper

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('-pid', type=str, help='Product ID IMG or MOD')
    parser.add_argument('-mode', type=str, help='Mode to be selected between roi based or csv based')
    parser.add_argument('-y', type=str, help='Year if mode is selected as csv')
    parser.add_argument('-dn', type=str, help='Daynight')
    parser.add_argument('-roi', type=str, help='lon_min,lat_min,lon_max,lat_max')
    parser.add_argument('-sd', type=str, help='Start Date if mode is selected as roi')
    parser.add_argument('-ed', type=str, help='Start Date if mode is selected as roi')
    parser.add_argument('-rs', type=bool, help='Resume downloading', default=False)
    args = parser.parse_args()
    mode = args.mode
    product_id = args.pid
    rs = args.rs
    dn = []
    for i in range(len(args.dn)):
        dn.append(args.dn[i])
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
    num_processes = 8

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
            tasks = get_json_tasks(id, start_date, duration, area_of_interest, products_id, dn, dir_json, collection_id)
            tasks2 = get_client_tasks(id, start_date, duration, products_id, dn, dir_json, dir_nc, collection_id)
            with multiprocessing.Pool(processes=4) as pool:
                results = list(pool.imap_unordered(json_wrapper, tasks))
            print(results)
            with multiprocessing.Pool(processes=4) as pool:
                results = list(pool.imap_unordered(client_wrapper, tasks2))
            print(results)
    elif mode == 'error':
        year = args.y
        filename = 'roi/us_fire_' + year + '_out_new.csv'
        df = pd.read_csv(filename)
        df = df.sort_values(by=['Id'])
        with open('log/error', 'r') as f:
            file = f.read().split('\n')

        def get_id(dir_str):
            return dir_str.split('/')[1]
        target_ids = list(map(get_id, file))

        def get_date(dir_str):
            return dir_str.split('/')[-1][:10]
        target_dates = list(map(get_date, file))

        for i, target_id in enumerate(target_ids):
            df_row = df[df.Id==int(target_id)]
            lon, lat, start_date = df_row['lon'].values.astype(float), df_row['lat'].values.astype(float), target_dates[i]
            roi_size = 1
            roi = [lon - roi_size, lat - roi_size, lon + roi_size, lat + roi_size]
            area_of_interest = 'W'+str(roi[0])+' '+'N'+str(roi[3])+' '+'E'+str(roi[2])+' '+'S'+str(roi[1])
            print('Currently processing id {}'.format(target_id))
            duration = datetime.timedelta(days=1)
            tasks = get_json_tasks(target_id, start_date, duration, area_of_interest, products_id, dn, dir_json, collection_id)
            tasks2 = get_client_tasks(target_id, start_date, duration, products_id, dn, dir_json, dir_nc, collection_id)
            with multiprocessing.Pool(processes=4) as pool:
                results = list(pool.imap_unordered(json_wrapper, tasks))
            print(results)
            with multiprocessing.Pool(processes=4) as pool:
                results = list(pool.imap_unordered(client_wrapper, tasks2))
            print(results)
    elif mode == 'roi':
        roi_arg = args.roi
        start_date = args.sd
        end_date = args.ed
        roi = [float(roi_arg.split(',')[0]), float(roi_arg.split(',')[1]), float(roi_arg.split(',')[2]), float(roi_arg.split(',')[3])]
        duration = datetime.datetime.strptime(end_date, '%Y-%m-%d') - datetime.datetime.strptime(start_date, '%Y-%m-%d')
        area_of_interest = 'W' + str(roi[0]) + ' ' + 'N' + str(roi[3]) + ' ' + 'E' + str(roi[2]) + ' ' + 'S' + str(
            roi[1])
        id = 'Alberta'
        print('Currently processing id {}'.format(id))
        tasks = get_json_tasks(id, start_date, duration, area_of_interest, products_id, dn, dir_json, collection_id)
        tasks2 = get_client_tasks(id, start_date, duration, products_id, dn, dir_json, dir_nc, collection_id)
        with multiprocessing.Pool(processes=4) as pool:
            results = list(pool.imap_unordered(json_wrapper, tasks))
        with multiprocessing.Pool(processes=4) as pool:
            results = list(pool.imap_unordered(client_wrapper, tasks2))
    elif mode == 'roi_from_csv':
        fire_name = args.y
        if fire_name not in ['quebec_fire', 'alberta_fire']:
            raise 'For roi_from_csv mode quebec_fire or alberta_fire is expected from year arguments'
        filename = 'roi/'+fire_name+'.csv'
        df = pd.read_csv(filename)
        ids, start_dates, end_dates, bottom_lefts, top_rights = df['system:index'].values.astype(str), df['start_date'].values.astype(str), df['end_date'].values.astype(str), df['bottom_left'].values.astype(str), df['top_right'].values.astype(str)
        for i, id in enumerate(ids):
            roi = (bottom_lefts[i] + top_rights[i]).replace('[', '').split(']')[0].split(', ')+(bottom_lefts[i] + top_rights[i]).replace('[', '').split(']')[1].split(', ')
            start_date = start_dates[i].split('T')[0]
            end_date = end_dates[i].split('T')[0]
            duration = datetime.datetime.strptime(end_date, '%Y-%m-%d') - datetime.datetime.strptime(start_date, '%Y-%m-%d')
            area_of_interest = 'W' + str(roi[0]) + ' ' + 'N' + str(roi[3]) + ' ' + 'E' + str(roi[2]) + ' ' + 'S' + str(roi[1])
            id = fire_name+'_'+id
            print('Currently processing id {}'.format(id))
            tasks = get_json_tasks(id, start_date, duration, area_of_interest, products_id, dn, dir_json, collection_id)
            tasks2 = get_client_tasks(id, start_date, duration, products_id, dn, dir_json, dir_nc, collection_id)
            with multiprocessing.Pool(processes=4) as pool:
                results = list(pool.imap_unordered(json_wrapper, tasks))
            with multiprocessing.Pool(processes=4) as pool:
                results = list(pool.imap_unordered(client_wrapper, tasks2))
    elif mode == 'location':
        import yaml
        with open("roi/configuration.yml", "r", encoding="utf8") as f:
            config = yaml.load(f, Loader=yaml.FullLoader)
        locations = ['thomas_fire', 'kincade_fire']
        for location in locations:
            print(location)
            rectangular_size = config.get('rectangular_size')
            latitude = config.get(location).get('latitude')
            longitude = config.get(location).get('longitude')
            start_date = config.get(location).get('start').strftime('%Y-%m-%d')
            roi = [longitude - rectangular_size, latitude - rectangular_size,
                 longitude + rectangular_size, latitude + rectangular_size]
            area_of_interest = 'W' + str(roi[0]) + ' ' + 'N' + str(roi[3]) + ' ' + 'E' + str(roi[2]) + ' ' + 'S' + str(roi[1])
            duration = datetime.timedelta(days=1)
            tasks = get_json_tasks(location, start_date, duration, area_of_interest, products_id, dn, dir_json, collection_id)
            tasks2 = get_client_tasks(location, start_date, duration, products_id, dn, dir_json, dir_nc, collection_id)
            with multiprocessing.Pool(processes=4) as pool:
                results = list(pool.imap_unordered(json_wrapper, tasks))
            with multiprocessing.Pool(processes=4) as pool:
                results = list(pool.imap_unordered(client_wrapper, tasks2))
    else:
        raise('No Support Mode')
