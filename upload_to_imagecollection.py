import datetime
import os
import subprocess
from glob import glob
from google.cloud import storage


def get_time_start(date_string):
    year = date_string[:4]
    day = datetime.date.fromisoformat(year+'-01-01') + datetime.timedelta(days=int(date_string[4:7])-1)
    hour = date_string[7:9]
    min = date_string[9:]
    return str(day)+'T'+hour+':'+min+':'+'00'


if __name__ == '__main__':
    file_dir = 'data/VNPIMGTIF'
    # file_list = glob(file_dir+'/*/*/*.tif')
    # file_list.sort()
    # if not os.path.exists('data\\cogtif'):
    #     os.mkdir('data\\cogtif')
    # for file in file_list:
    #     date = file.split('\\')[-3]
    #     time = file.split('\\')[-2]
    #     file_name = file.split('\\')[-1]
    #     print(date+'/'+time+'/'+file_name)
    #     if not os.path.exists('data\\cogtif\\'+date):
    #         os.mkdir('data\\cogtif\\'+date)
    #     os.system('rio cogeo create '+ file +' data\\cogtif\\'+'\\'+date+'\\'+file_name)
    file_list = glob('data\\cogtif/*/*.tif')
    file_list.sort()
    # for file in file_list:
    #     date = file.split('\\')[-2]
    #     file_name = file.split('\\')[-1]
    #     upload_cmd = 'gsutil cp '+file.replace('\\','/')+' gs://ai4wildfire/'+'VNPIMGTIF/2021'+date+'/'+file_name
    #     os.system(upload_cmd)
        # subprocess.call(upload_cmd.split())

    for file in file_list:
        date = file.split('\\')[-2]
        file_name = file.split('\\')[-1]
        time_start = get_time_start(file_name[7:-4])
        
        cmd = 'earthengine upload image --time_start '+time_start+' --asset_id=projects/grand-drive-285514/assets/viirs_375/'+file.split('\\')[-1][:-4]+' --pyramiding_policy=sample gs://ai4wildfire/'+'VNPIMGTIF/2021'+date+'/'+file_name
        subprocess.call(cmd.split())