import os
import yaml
import pandas as pd

if __name__=='__main__':
    years = ['2017', '2018', '2019', '2020']
    for year in years:
        filename = 'roi/us_fire_'+year+'.csv'
        df = pd.read_csv(filename)
        df = df.drop(columns=['.geo', 'FDate', 'IDate', 'system:index'])
        df['start_date'] = pd.to_datetime(df.start_date)
        df['start_date'] = df['start_date'].dt.strftime('%Y-%m-%d')
        df['end_date'] = pd.to_datetime(df.end_date)
        df['end_date'] = df['end_date'].dt.strftime('%Y-%m-%d')
        df.set_index('Id', inplace=True)
        output_name = 'roi/us_fire_'+year+'_out_new.csv'
        df.to_csv(output_name)