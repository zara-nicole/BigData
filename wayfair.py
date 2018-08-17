# -*- coding: utf-8 -*-
"""
Created on Thu Jun 07 14:25:33 2018

@author: zsaldanh
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import glob
import re

file = 'trafficsources/wayfair.com.csv'
data = pd.read_csv(file)

#convert month to date or category?
#data['month'] = pd.to_datetime(data['month'],format='%Y-%m')
#data = data.rename(columns={'month':'year_month'})
#data['month'] = data.year_month.str[5:]
#data['year'] = data.year_month.str[0:4]
#split month into month and year
def split_month(df) :
    df = df.rename(columns={'month':'year_month'})
    df['month'] = df.year_month.str[5:]
    df['year'] = df.year_month.str[0:4]
    return df
def convert_channel(keyword):
    #looks for keyword in url and replaces it with the keyword
    prog = re.compile('^.*'+ keyword + '.*$')
    data['channel'] = data.channel.str.replace(prog,keyword)
def convert_channel2(keyword,site):
    #looks for keyword in url and replaces it with site
    prog = re.compile('^.*'+ keyword + '.*$')
    data['channel'] = data.channel.str.replace(prog,site)
def remove_end(keyword):
    #removes keyword input such as '.com' or '.net'
    data['channel'] = data.channel.str.rstrip(keyword)
    
def channel_share(date):
    df = data[data['year_month']== date].groupby('channel').sum()
    test = df.sum()
    df_ = pd.DataFrame(columns=["share"],index=np.array(["total share"]))
    df_["share"] = test.values
    df1 = df.append(df_)
    val = 1-df1.share[-1]
    df2 = pd.DataFrame(columns=["share"],index = np.array(["other"]))
    df2["share"] = val
    df1 = df.append(df2).append(df_)
    return df1

def clean_df(year):
    df_ = df_clean.loc[year]
    pivot = df_.pivot('channel','year_month','share')
    for column in pivot:
        pivot_new = pivot[pd.notnull(pivot[column])]
    return pivot_new
#def combine_channel2(channel):
    #takes name of channel and combines shares within the same year into one row
split_month(data)
convert_channel('pinterest')
convert_channel('facebook')
convert_channel2('messenger','facebook')
convert_channel('amazon')
convert_channel('google')
convert_channel('twitter')
convert_channel('outlook')
convert_channel('whatsapp')
remove_end('.com')
convert_channel2('instagra','instagram')


pivot_all = df_clean.pivot('channel','year_month','share').dropna()
pivot_all.to_excel(writer,'Wayfair2016')
writer.save()
pivot_2016 = df_clean.loc['2018'].pivot('channel','year_month','share').dropna()
#each channel's shares summed by year-month
#sum_shares = data.groupby(['year_month','channel']).sum().reset_index().sort_values(by=['year_month','share'],ascending=False)
#df = df.set_index('year_month')
#t_format = '%Y-%m'
#df.year_month = pd.to_datetime(df.year_month)
#time_series = pd.Series(df,index=year_month)


#date_times = pd.to_datetime(df.year_month)
#df_clean = df.set_index(date_times)
#df_month = df_clean.resample('M').mean()

#==============================================================================
# #chart for 2016 data, x=month, y=share
def wayf_2016():
     df_2016 = df_clean.loc['2016'].pivot('channel','year_month','share').dropna()
     #Line Plot of Data
     x = range(3)
     df1 = pivot_new.copy()
     df1=df1.transpose()
     df1=df1.reset_index()
# 
# plt.plot( x, 'facebook', data=df1, marker='o', markerfacecolor='blue', markersize=8, color='skyblue', linewidth=3)
# plt.plot( x, 'linkedin', data=df1, marker='o', markerfacecolor='blue', markersize=8, color='blue', linewidth=3)
# plt.plot( x, 'google', data=df1, marker='o', markerfacecolor='blue', markersize=8, color='red', linewidth=3)
# plt.plot( x, 'pinterest', data=df1, marker='o', markerfacecolor='blue', markersize=8, color='pink', linewidth=3)
# plt.plot( x, 'reddit', data=df1, marker='o', markerfacecolor='blue', markersize=8, color='yellow', linewidth=3)
# plt.plot( x, 'twitter', data=df1, marker='o', markerfacecolor='blue', markersize=8, color='green', linewidth=3)
# plt.plot( x, 'youtube', data=df1, marker='o', markerfacecolor='blue', markersize=8, color='purple', linewidth=3)
# plt.legend()
# plt.show()
# #Stacked Area Chart
# plt.clf()
# x=range(3)
# y = pivot_new.values.tolist()
# 
# plt.stackplot(x,y, labels=x)
# plt.legend(loc='upper left')
# plt.show()
# df1 = df1.reset_index()
#==============================================================================




#plt.style.use('seaborn-darkgrid')
#my_dpi=96
#plt.figure(figsize=(480/my_dpi, 480/my_dpi), dpi=my_dpi)
 
# multiple line plot
#for column in pivot_new.drop(axis=1):
#plt.plot(pivot_new['x'], pivot_new[column], marker='', color='grey', linewidth=1, alpha=0.4)
 
writer  = pd.ExcelWriter('Wayfair.xlsx')
#pivot_new.to_excel(writer,'Wayfair2016')
#writer.save()
#chart for 2017 data
#df_2017 = df_clean.loc['2017'].pivot('channel','year_month','share').dropna()
#pivot_new.to_excel(writer,'Wayfair2017')
#writer.save()

#pivot.to_excel(writer,'Wayfair2018')
#writer.save()

#df['year_month'] = df.index[][0]
#df['channel'] = df.index[][1]
#new_col = [df.index[i][0] for i in df]

#2016-2018, all data. graph

#pivot.to_excel(writer,'WayfairAll')
#writer.save()

pivot_all = df_clean.pivot('channel','year_month','share').dropna()
pivot_all.to_excel(writer,'Wayfair2016')
writer.save()
pivot_2016 = df_clean.loc['2018'].pivot('channel','year_month','share').dropna()