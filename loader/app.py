import pandas as pd
import numpy as np
import h5py as h5
path = '/root/data/BraTS2020_training_data/content/data/volume_47_slice_68.h5'

def readFilesH5(path):
    h5file = h5.File(path,'r')
    dfImage = pd.DataFrame(np.array(h5file['image']))
    dfMask = pd.DataFrame(np.array(h5file['mask']))
    h5file.close()
    # keys = [k for k in data.keys()]
    # print(keys)
    print(dfImage)
    print(dfMask[0:2,0:2,:])


if __name__ == '__main__':
    readFilesH5(path)