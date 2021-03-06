import os, sys
import rdkit
from rdkit import Chem
from os.path import expanduser
home = expanduser("~")
import sys,csv
import PIL
from PIL import Image
from itertools import islice
from IPython import display
import pandas as pd
import numpy as np
import itertools
import scipy as sp
import multiprocessing
import subprocess, os, signal, sys
import h5py
import random

data_dir = '/srv/nas/mk1/users/lgendele/Amanda_Screen/'
dump_dir = '/srv/home/lpinto/data/'
# specify the size of 
sample_size = 1000
frame_size = 8
#train_test_split = 0.5

df_ROI = pd.read_csv('/srv/home/lpinto/data-processing/Amanda_all_A_rois.csv')
# exclude the sauronx drugs
legacy_plates = df_ROI[~df_ROI['project'].str.contains('sauronx')]['run'].unique().tolist()
#split train test here
random.shuffle(legacy_plates)
train_plates = legacy_plates[0:len(legacy_plates)/2]
test_plates = legacy_plates[len(legacy_plates)/2:len(legacy_plates)]
train_plates = [str(x) for x in train_plates]
test_plates = [str(x) for x in test_plates]
print("Legacy plates: " + str(len(legacy_plates)))
print("training size: " + str(len(train_plates)))
print("testing size: " + str(len(test_plates)))
assays_wanted = ["background","softTap1","ASR1","ASRtrain","VSR1","VSR2","VSR3","VSR4","ASRi","solidRed","VSRASR"]

def make_8_frame_to_hdf5(plates_wanted,learning_tag):
	iter_count = 0
	h5f = h5py.File(dump_dir + learning_tag  + '.hdf5','w')

	while iter_count < sample_size:
		plate = str(random.sample(plates_wanted,1))[2:-2]
		plate_tag = data_dir+plate
		all_assays = os.listdir(plate_tag)
		assays_to_sample = []
		#include only assays in assays_wanted
		for aw in assays_wanted:
			index = [idx for idx, s in enumerate(all_assays) if aw in s][0]
			assays_to_sample.append(all_assays[index])

		assay = str(random.sample(assays_to_sample,1))[2:-2]
		assay_tag = plate_tag+'/'+assay
		jpgs_to_sample = os.listdir(assay_tag)				
		eight_jpgs = []

		start_idx = random.randint(0,len(jpgs_to_sample)-1-frame_size)

		for x in range(start_idx,start_idx+frame_size): eight_jpgs.append(assay_tag+'/'+jpgs_to_sample[x])
		# making huge np array here
		eight_frame_chunk = np.array([np.array(Image.open(fname)) for fname in eight_jpgs]) 
		single_plate = df_ROI[df_ROI['run'] == int(plate)]
				
		well = random.sample(single_plate['well_index'].unique(),1)
		chosen_well = single_plate[single_plate['well_index'] == well]
		#print(chosen_well)
		x1 = chosen_well['x0'].values[0]
		x2 = chosen_well['x1'].values[0]
		y1 = chosen_well['y0'].values[0]
		y2 = chosen_well['y1'].values[0]
		drug = chosen_well['key'].values[0]
		# np array indexed in (z,y,x) fashion
		print("size of numpy: " + str(eight_frame_chunk.shape))
		
		eight_frame_chunk_well = eight_frame_chunk[:,min(y1,y2):max(y1,y2),min(x1,x2):max(x1,x2)]
					

		jpeg_names = ' '.join(eight_jpgs)
		jpeg_names = jpeg_names.replace("/","_")
		save_tag = plate + "_" + assay + '_' + str(drug) + "_well_" + str(jpgs_to_sample[start_idx])
		#print(save_tag)
		#print(type(save_tag))
		#print(eight_frame_chunk_well)
		#print(type(eight_frame_chunk_well))
		name = "name_" + str(well)
		h5f.create_dataset(save_tag,data=eight_frame_chunk_well,dtype='i8')
		iter_count +=1

	h5f.close()

					

def main():
    print("Initializng 2 workers")
    original_sigint_handler = signal.signal(signal.SIGINT, signal.SIG_IGN)
    pool = multiprocessing.Pool(processes=16)
    signal.signal(signal.SIGINT, original_sigint_handler)
    try:
        #results = pool.map_async(make_8_frame_to_hdf5,2)
        make_8_frame_to_hdf5(train_plates,"training_set")
	make_8_frame_to_hdf5(test_plates,"validation_set")
	print("Waiting for results")
        #results.get(60000000000) # Without the timeout this blocking call ignores all signals.
    except KeyboardInterrupt:
        print("Caught KeyboardInterrupt, terminating workers")
        pool.terminate()
    else:
        print("Normal termination")
        pool.close()
    pool.join()

if __name__ == "__main__":
    main()
