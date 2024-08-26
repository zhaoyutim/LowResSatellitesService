import os
import numpy as np
import torch
from spatial_models.unet import UNet
from spatial_models.attentionunet import AttentionUnet
from monai.data import decollate_batch, DataLoader, Dataset
from monai.transforms import Activations, AsDiscrete, Compose
from spatial_models.swinunetr.swinunetr import SwinUNETR
from spatial_models.unetr.unetr import UNETR
from torch import nn, optim
import glob
from pathlib import Path
root_path = str(Path(__file__).resolve().parents[0]) + "/"


class Normalize(object):
    def __init__(self, mean, std):
        self.mean = mean
        self.std = std

    def __call__(self, sample):
        for i in range(len(self.mean)):
            sample[i, :, ...] = (sample[i, :, ...] - self.mean[i]) / self.std[i]
        return sample
    
class InferenceDataset(Dataset):
    def __init__(self, path, transform=None):
        self.transform = transform
        self.path = path
        self.len = len(np.load(self.path, mmap_mode='r'))

    def __len__(self):
        return self.len

    def __getitem__(self, idx):
        item = torch.from_numpy(np.load(self.path, mmap_mode='r')[idx].copy().astype(np.float32)).squeeze(0)
        item = self.transform(item)
        return item

def run(model_name,mode,batch_size,num_heads,hidden_size,n_channel,ts_length,attn_version,start_date,end_date,output_path,data_path,checkpoint_path,plot=False):
    if mode != 'af':
        transform = Normalize(mean = [17.952442,26.94709,19.82838,317.80234,308.47693,13.87255,291.0257,288.9398],
            std = [15.359564,14.336508,10.64194,12.505946,11.571564,9.666024,11.495529,7.9788895])
    else:
        transform = Normalize(mean = [18.76488,27.441864,20.584806,305.99478,294.31738,14.625097,276.4207,275.16766],
            std = [15.911591,14.879259,10.832616,21.761852,24.703484,9.878246,40.64329,40.7657])

    device = torch.device("cuda:9" if torch.cuda.is_available() else "cpu")
    num_classes = 2
    image_size = (ts_length, 256, 256)
    patch_size = (1, 2, 2)
    window_size = (ts_length, 4, 4)
    if model_name == 'unet3d':
        model = UNet(spatial_dims=3, in_channels=n_channel, out_channels=num_classes, channels=(64, 128, 256, 512, 1024), strides=(1,2,2))
    elif model_name == 'attunet3d':
        model = AttentionUnet(spatial_dims=3, in_channels=n_channel, out_channels=num_classes, channels=(64, 128, 256, 512, 1024), strides=(1,2,2))
    elif model_name == 'unetr3d':
        model = UNETR(in_channels=n_channel, out_channels=num_classes, img_size=image_size, spatial_dims=3, norm_name='batch', feature_size=hidden_size, patch_size=(1,16,16))
    elif model_name == 'unetr3d_half':
        model = UNETR(in_channels=n_channel, out_channels=num_classes, img_size=image_size, spatial_dims=3, norm_name='batch', feature_size=hidden_size, patch_size=(1,16,16), hidden_size=384, mlp_dim = 1536)
    elif model_name == 'swinunetr3d':
        model = SwinUNETR(
        image_size=image_size,
        patch_size=patch_size,
        window_size=window_size,
        in_channels=n_channel,
        out_channels=2,
        depths=(2, 2, 2, 2),
        num_heads=(num_heads, num_heads, num_heads, num_heads),
        feature_size=hidden_size,
        norm_name='batch',
        drop_rate=0.0,
        attn_drop_rate=0.0,
        drop_path_rate=0.0,
        attn_version=attn_version,
        normalize=True,
        use_checkpoint=False,
        spatial_dims=3
    )
    else:
        raise 'not implemented'

    print('Number of Parameters:', sum(p.numel() for p in model.parameters())/1e6, "M")
    post_trans = Compose([Activations(sigmoid=True), AsDiscrete(threshold=0.5)])
    optimizer = optim.Adam(model.parameters())
    
    if plot:
        os.makedirs(root_path+'/evaluation_plot',exist_ok=True)

    checkpoint = torch.load(checkpoint_path,map_location='cpu')
    model.load_state_dict(checkpoint['model_state_dict'])
    optimizer.load_state_dict(checkpoint['optimizer_state_dict'])

    #from collections import OrderedDict
    #state_dict = torch.load(checkpoint_path, map_location='cpu')
    #model_state_dict = state_dict['model_state_dict']
    #new_state_dict = OrderedDict()
    #for k, v in model_state_dict.items():
    #    new_state_dict[k.replace("module.", "")] = v
    #model.load_state_dict(new_state_dict)
    #state_dict['model_state_dict'] = model.state_dict()
    #torch.save(state_dict,checkpoint_path)

    model = nn.DataParallel(model,device_ids=[9,8,7,6,5,4,3])
    model.to(device)

    model.eval() 
    def normalization(array):
        return (array-array.min()) / (array.max() - array.min())

    data_paths = []
    dates_list = list(np.arange(np.datetime64(start_date), np.datetime64(end_date)))
    for i in range(len(dates_list)//ts_length):
        date_interval = str(dates_list[i])+"-"+str(dates_list[i+ts_length-1])
        print(date_interval)
        data_paths.extend(glob.glob(data_path+'/'+date_interval+'*.npy', recursive=True))
    
    print("Found ", len(data_paths), " files." )
    
    save_path = output_path+'/'+ model_name + '/' + 'raw/'
    os.makedirs(save_path,exist_ok=True)

    for l in range(len(data_paths)):
        test_dataset = InferenceDataset(data_paths[l], transform)
        test_dataloader = DataLoader(test_dataset, batch_size=batch_size, shuffle=False)
        results = np.zeros(shape=(len(test_dataset),ts_length,256,256))
        
        idx=0
        for j, batch in enumerate(test_dataloader):
            outputs = model(batch.to(device))
            outputs = [post_trans(i) for i in decollate_batch(outputs)]
            outputs = np.stack(outputs, axis=0)

            if plot:
                import matplotlib.pyplot as plt
                for k in range(batch.shape[0]):
                    for i in range(ts_length):
                        output_ti = outputs[k, 1, i, :, :]>0.5                    
                        plt.imshow(normalization(batch[k, 3, i, :, :]), cmap='gray')
                        img_tp = np.where(output_ti==1, 1.0, 0.)
                        img_tp[img_tp==0.]=np.nan

                        plt.imshow(img_tp, cmap='autumn', interpolation='nearest')
                        plt.axis('off')

                        plt.savefig(root_path+'/evaluation_plot/model_{}_task_{}_nhead_{}_hidden_{}_nbatch_{}_nts_{}_ts_{}_nc_{}.png'.format(model_name, mode, num_heads, hidden_size, j, k, i, n_channel), bbox_inches='tight')
                        plt.show()
                        plt.close()
            
            results[idx:idx+outputs.shape[0],:,:,:] = outputs[:, 1, :, :, :]>0.5
            idx = idx+outputs.shape[0]
        roi = data_paths[l].split('/')[-1].split('_')[1:]
        roi[-1] = roi[-1][:-4]
        roi_string = '_'.join(roi)
        np.save(save_path + start_date +"-"+ end_date +"_"+roi_string, results)
        print("Inference for file", l, "completed.")

