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
    data = fs.pivot_df(data)
    sum_list = fs.compute_sum(data)
    other_list = [1-x for x in sum_list]
    data.loc['other'] = other_list
    name = filename[15:(filename[15:-4].index('.'))+15] + '_df'
        
    if name not in list_names:
        my_data[name] = data
        list_names.append(name)
    else:
        my_data[filename[15:-4]+'_df'] = data
        list_names.append(filename[15:-4]+'_df')
        
fb_data = pd.DataFrame()
for key,df in my_data.iteritems():
    if 'facebook' in df.index:
        new_df = df.loc['facebook'].rename(key.replace("_df","")).to_frame().reset_index().T
        new_df.columns = new_df.loc['year_month']
        new_df = new_df.drop(['year_month']) # (1,18)
        fb_data = fb_data.append(new_df)
        
fb_year = fb_data[['2017-03','2018-03']]
fb_year['perc_change'] = (fb_data['2018-03']-fb_data['2017-03'])*100
fb_year = fb_year.sort_values('perc_change',ascending=False).reset_index()

#get the mean of every column in fb_data
fb_avg_month = pd.DataFrame()
avg = []
for column in fb_data:
    avg.append(fb_data[column].mean())

#fb_avg_month = pd.DataFrame(avg,columns = fb_data.columns.values)

row = pd.Series(avg,fb_data.columns.values)
fb_avg_month = fb_avg_month.append([row],ignore_index=True)
fb_avg_month = fb_avg_month.transpose()