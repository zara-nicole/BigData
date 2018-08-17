# -*- coding: utf-8 -*-
"""
Created on Thu Jun 14 10:30:05 2018

@author: zsaldanh
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import glob
import re

def split_month(df) :
    #split month column into a seperate year and month column
    #rename old column to 'year_month'
    df = df.rename(columns={'month':'year_month'})
    df['month'] = df.year_month.str[5:]
    df['year'] = df.year_month.str[0:4]
    return df
#
#def convert_channel(df,keyword):
#    #looks for keyword in url and replaces it with the keyword
#    prog = re.compile('^.*'+ keyword + '.*$')
#    df['channel'] = df.channel.str.replace(prog,keyword)
#    return df

def convert_channel(df,sites):
    #looks for keyword in url and replaces it with the keyword
    for site in sites:
        prog = re.compile('^.*'+ site + '.*$')
        df['channel'] = df.channel.str.replace(prog,site)
    return df

def convert_channel2(df,keyword,site):
    #looks for keyword in url and replaces it with site
    prog = re.compile('^.*'+ keyword + '.*$')
    df['channel'] = df.channel.str.replace(prog,site)
    return df

def pivot_df(df):
    pivot = df.pivot('channel','year_month','share').dropna()
    return pivot

def compute_sum(df):
    list_of_sums = []
    for i in df.columns.values:
         list_of_sums.append(df[i].sum())
    return list_of_sums