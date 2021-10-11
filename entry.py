import argparse
import os
from datetime import datetime, timedelta

if __name__=='__main__':

    parser = argparse.ArgumentParser(description='assign processing date')
    parser.add_argument('--date', '-d', type=int, help='assign processing date')
    args = parser.parse_args()
    date = args.date

    for i in range(2):
        os.system('python main.py -d '+str(date+i))
        print('python main.py -d '+ str(date+i))