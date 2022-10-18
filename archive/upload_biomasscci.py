import os
import subprocess
from glob import glob
if __name__=='__main__':
    year='2018'
    path = os.path.join('/Users/zhaoyu/v3.0/geotiff', year, '*.tif')
    file_list = glob(path)
    for file in file_list:
        upload_cmd = 'gsutil cp ' + file + ' gs://ai4wildfire/' + 'biomasscci/' + year + '/' + file
        os.system(upload_cmd)
        cmd = 'earthengine upload image --time_start '+year+'-01-01T00:00' + ' --asset_id=projects/ee-zhaoyutim/assets/biomasscci'+year+'/' + \
              file.split('/')[-1][
              :-4] + ' --pyramiding_policy=sample gs://ai4wildfire/' + 'biomasscci/' + year + '/' + file
        subprocess.call(cmd.split())
