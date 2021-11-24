import os

from osgeo import gdal

from ProcessingPipeline.processing_pipeline import Pipeline


def test_crop_to_roi(date,roi,file):


    pipeline = Pipeline()
    pipeline.crop_to_roi(date,roi,file)

if __name__=='__main__':
    date = '2020-09-01'
    os.chdir('..')
    file = 'data/cogtif/2020-09-01/VNPIMG2020-09-01-2054.tif'
    roi = [-130.23, 32.56, -113.10, 60.01]
    test_crop_to_roi(date, roi, file)
    print(gdal.Info(file.replace('cogtif', 'cogsubset')))
    os.remove(file.replace('cogtif', 'cogsubset'))