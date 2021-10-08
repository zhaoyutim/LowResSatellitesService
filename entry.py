import argparse
import os
from datetime import datetime, timedelta

if __name__=='__main__':

    parser = argparse.ArgumentParser(description='assign processing date')
    parser.add_argument('--date', '-d', type=str, help='assign processing date')
    args = parser.parse_args()
    date = args.date

    day_time = datetime.strptime('09:00', '%H:%M')
    night_time = datetime.strptime('20:00', '%H:%M')
    delta = timedelta(minutes=6)
    for i in range(20):
        input_time = (night_time+delta*i).strftime('%H%M')
        os.system('python main.py -d '+date+' -t '+input_time)
        print('python main.py -d '+date+' -t '+input_time)

        input_time = (day_time+delta*i).strftime('%H%M')
        os.system('python main.py -d '+date+' -t '+input_time)
        print('python main.py -d '+date+' -t '+input_time)