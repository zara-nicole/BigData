# -*- coding: utf-8 -*-
"""
Created on Mon Jun 25 12:12:43 2018

@author: zsaldanh
"""

# -*- coding: utf-8 -*-
"""
Created on Thu Jun 14 09:26:22 2018

@author: zsaldanh
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os
import seaborn as sns
import glob
import re
import functions_social as fs
        
df_all = pd.DataFrame()
my_data = {}
list_names = []
for filename in glob.glob('trafficsources/*'):
    #creates dictionary of all the dataframes of the csv files where sites with at least one nan is dropped
    data = pd.read_csv(filename)
    data = fs.split_month(data).iloc[:,0:4]
    site_list = ['pinterest','facebook','amazon','google','twitter','outlook','linkedin','reddit',
                 'youtube', 'whatsapp','instagram','stackexchange','slideshare','vk','getpocket','stackoverflow','quora',
                'netvibes','soundcloud','wikia','digg','vimeo','meetup']
    #?print top 6 of each before pivoting and dropping nans, compare to final list
    #maybe create list of 'honorable mentions'
    data = fs.convert_channel(data,site_list)
    data = data.groupby(['year_month','channel']).sum().reset_index().sort_values(by=['year_month','share'],ascending=False)
    data = data = data.pivot('channel','year_month','share')
    sum_list = fs.compute_sum(data)
    other_list = [1-x for x in sum_list]
    #data.loc['other'] = other_list
    name = filename[15:(filename[15:-4].index('.'))+15] + '_df'
        
    if name not in list_names:
        my_data[name] = data
        list_names.append(name)
    else:
        my_data[filename[15:-4]+'_df'] = data
        list_names.append(filename[15:-4]+'_df')
        