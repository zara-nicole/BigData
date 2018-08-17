# -*- coding: utf-8 -*-
"""
Created on Mon Jul 09 09:42:45 2018

@author: zsaldanh
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os
import seaborn as sns
import glob
import re
from datetime import datetime
import matplotlib.pyplot as plt
import numpy as np

def import_data(file_name):
#input: csv file name as a string
    data = pd.read_csv(file_name)
    data['As Of Date'] = pd.to_datetime(data['As Of Date'], format = "%m/%d/%Y %H:%M")
    
def describe_data(df):
    print df['Ticker Symbol'].unique()
    print df['Username'].unique()
    
def compare_likes(df,name1,name2):
    #input: dataframe, usernames to compare (printed in describe_data)
    #example: compare_likes(data,'jackinthebox','burgerking')
    fig, ax1 = plt.subplots()
    fig.suptitle('Facebook Likes Over Time')

    color = 'tab:red'
    ax1.set_xlabel('Time (months)')
    ax1.set_ylabel(name1, color=color)
    df_1 = data[data['Username']==name1]
    ax1.plot(df_1['As Of Date'],df_1['Likes'], color=color)
    ax1.tick_params(axis='y', labelcolor=color)
    
    ax2 = ax1.twinx()  # instantiate a second axes that shares the same x-axis
    color = 'tab:blue'
    ax2.set_ylabel(name2, color=color)  # we already handled the x-label with ax1
    df_2 = data[data['Username']==name2]
    ax2.plot(df_2['As Of Date'],df_2['Likes'], color=color)
    ax2.tick_params(axis='y', labelcolor=color)
    fig.tight_layout()  # otherwise the right y-label is slightly clipped
    plt.show()
    
def compare_likes_date(df,name1,name2,date):
    #same as above but shows from a certain date onward
    #example: compare_likes(data,'jackinthebox','burgerking','2015-06')
    fig, ax1 = plt.subplots()
    fig.suptitle('Facebook Likes Over Time')

    color = 'tab:red'
    ax1.set_xlabel('Time (months)')
    ax1.set_ylabel(name1, color=color)
    df_1 = data[data['Username']==name1]
    df_1 = df_1[df_1['As Of Date'] >= date]
    ax1.plot(df_1['As Of Date'],df_1['Likes'], color=color)
    ax1.tick_params(axis='y', labelcolor=color)
    ax2 = ax1.twinx()  # instantiate a second axes that shares the same x-axis
    color = 'tab:blue'
    ax2.set_ylabel(name2, color=color)  # we already handled the x-label with ax1
    df_2 = data[data['Username']==name2]
    df_2 = df_2[df_2['As Of Date'] >= date]
    ax2.plot(df_2['As Of Date'],df_2['Likes'], color=color)
    ax2.tick_params(axis='y', labelcolor=color)
    fig.tight_layout()  # otherwise the right y-label is slightly clipped
    plt.show()