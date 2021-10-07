import os
from datetime import datetime, timedelta

if __name__=='__main__':

    date = '191'
    day_time = datetime.strptime('08:00', '%H:%M')
    night_time = datetime.strptime('20:00', '%H:%M')
    delta = timedelta(minutes=6)
    for i in range(30):
        input_time = (night_time+delta*i).strftime('%H%M')
        os.system('python main.py -d '+date+' -t '+input_time)
        print('python main.py -d '+date+' -t '+input_time)

        input_time = (day_time+delta*i).strftime('%H%M')
        os.system('python main.py -d '+date+' -t '+input_time)
        print('python main.py -d '+date+' -t '+input_time)