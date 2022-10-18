import numpy as np
import datetime
from glob import glob
from pathlib import Path
import cv2
import matplotlib.pyplot as plt

path = 'R12068'

if __name__=='__main__':
    data_path = Path(path)
    data_file_list = glob(str(data_path / "*.png"))
    data_file_list.sort()
    background = cv2.imread('background_2.png')
    background = background[round(background.shape[0]*0.33):round(background.shape[0]*0.85),round(background.shape[1]*0.1):round(background.shape[1]*0.85)]
    for i in range(len(data_file_list)):
        time = data_file_list[i][-14:-4]
        bk_img = cv2.imread(data_file_list[i], cv2.IMREAD_UNCHANGED)
        bk_img = bk_img[round(bk_img.shape[0]*0.33):round(bk_img.shape[0]*0.80),round(bk_img.shape[1]*0.11):round(bk_img.shape[1]*0.83)]
        bk_img = cv2.resize(bk_img, dsize=(background.shape[1],background.shape[0]), interpolation=cv2.INTER_CUBIC)

        cv2.putText(bk_img, 'Tweedsmuir Complex Fire, BC ' + str(time), (30, 40),
                    cv2.FONT_HERSHEY_SIMPLEX,
                    0.6, (255, 255, 255, 255), 2, cv2.LINE_AA)

        cv2.imwrite(path + '_output/' + path + str(time) + '.png', bk_img,[int(cv2.IMWRITE_PNG_COMPRESSION), 9])
    cv2.imwrite(path+'_output/' + 'background.png',background)