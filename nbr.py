import numpy as np
from matplotlib import pyplot as plt

from Preprocessing.PreprocessingService import PreprocessingService

path = 'modispan.tif'
if __name__=='__main__':
    preprocessing = PreprocessingService()
    tiff, _ = preprocessing.read_tiff(path)
    plt.imshow((tiff[0,:,:]-tiff[1,:,:])/(tiff[0,:,:]+tiff[1,:,:]))
    plt.show()