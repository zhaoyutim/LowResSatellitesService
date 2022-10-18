import datetime
import glob

import matplotlib.pyplot as plt

from Preprocessing.PreprocessingService import PreprocessingService

if __name__ == '__main__':
    path = 'data/cogtif/'
    start_date = '2018-07-12'
    viz_params = {"red_band":4,"green_band":4,"blue_band":4}
    preprocessing = PreprocessingService()
    for i in range(1):
        date = (datetime.datetime.strptime(start_date, '%Y-%m-%d') + datetime.timedelta(i)).strftime('%Y-%m-%d')
        file_list = glob.glob(path+start_date+'/*.tif')
        file_list.sort()
        for file in file_list:
            array, _ = preprocessing.read_tiff(file)
            composite_img = preprocessing.get_composition(array, viz_params)
            plt.imshow(composite_img.transpose((1,2,0)))
            plt.show()