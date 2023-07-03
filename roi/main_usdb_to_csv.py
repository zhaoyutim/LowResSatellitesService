import pandas as pd
if __name__=='__main__':
    df = pd.read_json('MTBS_US_2021_events.json', orient='index')
    df = df[['BurnBndAc', 'BurnBndLat', 'BurnBndLon', 'modisStartDate', 'modisEndDate']].reset_index(level=0).rename(
        columns={'index': 'Id', 'BurnBndAc': 'area', 'BurnBndLat': 'lat', 'BurnBndLon': 'lon',
                 'modisStartDate': 'start_date', 'modisEndDate': 'end_date'})
    df = df[df.start_date.notna()&df.end_date.notna()]
    df.to_csv('us_fire_2021_out_new.csv', index=False)