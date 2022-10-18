from satpy import Scene, MultiScene
from glob import glob
from satpy import find_files_and_readers
from satpy import available_readers
from satpy.composites import GenericCompositor
from satpy.multiscene import timeseries
from satpy.writers import to_image
import numpy as np
import imageio

if __name__=='__main__':
    print(available_readers())
    # define path to FCI test data folder
    location = 'fagel'
    path_to_data = '/Users/zhaoyu/PycharmProjects/LowResSatellitesService/data/' + location

    mscn = MultiScene.from_files(glob('/Users/zhaoyu/PycharmProjects/LowResSatellitesService/data/'+location+'/*.nat'), reader='seviri_l1b_native')
    mscn.load(['IR_039', 'IR_108', 'IR_120'])
    # mscn.save_animation('ll.mp4', fps=10)

    #bbox = (14.63, 61.37, 15.63, 62.37)
    bbox = (13.92, 61.47, 14.92, 62.47)
    croped_mscn = mscn.crop(ll_bbox=bbox)
    blended_scene = mscn.blend(blend_function=timeseries)
    ir_039 = blended_scene['IR_039']
    ir_108 = blended_scene['IR_108']
    ir_120 = blended_scene['IR_120']
    img_asnp = np.zeros((ir_039.shape[0],ir_039.shape[1],ir_039.shape[2],3))
    img_asnp[:, :, :, 0] = (ir_039 - ir_039.min()) / (ir_039.max() - ir_039.min())
    img_asnp[:, :, :, 1] = (ir_108 - ir_108.min()) / (ir_108.max() - ir_108.min())
    img_asnp[:, :, :, 2] = (ir_120 - ir_120.min()) / (ir_120.max() - ir_120.min())
    imageio.mimsave('./full_img.gif', (np.nan_to_num(img_asnp)*255).astype(int), fps=5, format='GIF')