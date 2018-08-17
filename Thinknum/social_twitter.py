# -*- coding: utf-8 -*-
"""
Created on Fri Jul 13 10:16:35 2018

@author: zsaldanh
"""

import pandas as pd
import json
import thinknum_pull
import pandas as pd
import matplotlib.pyplot as plt

dataset_name = 'social_twitter'

#GET ALL THE COMPANIES COVERED WITH MIX ADDED & MAX UPDATED DATE
def single_ticker(ticker):    
    form_data = {
        "request": json.dumps({
                     "tickers": [],
                     "filters": [
                   {
                     "column": "dataset__entity__entity_ticker__ticker__ticker",
                     "type": "=",
                     "value": [ticker]
                   }],
                    }),
            'start': 1,
            'limit': 1000,
    }    
    
    items_df = thinknum_pull._get_data_multi_loop(dataset_name, form_data)
    items_df['As Of Date'] = pd.to_datetime(items_df['As Of Date'],format = "%Y-%m-%d %H:%M:%S")
    return items_df

def multi_tickers(dict_names):
    ticker = dict_names
    form_data = {
        "request": json.dumps({
                     "tickers": [],
                     "filters": [
                   {
                     "column": "dataset__entity__entity_ticker__ticker__ticker",
                     "type": "=",
                     "value": ticker
                   }],
                    }),
            'start': 1,
            'limit': 1000,
    }    
    
    items_df = thinknum_pull._get_data_multi_loop(dataset_name, form_data)
    items_df['As Of Date'] = pd.to_datetime(items_df['As Of Date'],format = "%Y-%m-%d %H:%M:%S")
    return items_df


def compare_col(df,col,name1,name2):
    #input: dataframe, usernames to compare (printed in describe_data)
    #example: compare_col(data,'Followers','jackinthebox','burgerking')
    fig, ax1 = plt.subplots()
    fig.suptitle('Twitter Followers Over Time')

    color = 'tab:red'
    ax1.set_xlabel('Time (months)')
    ax1.set_ylabel(name1, color=color)
    df_1 = df[df['Screen Name']==name1]
    ax1.plot(df_1['As Of Date'],df_1[col], color=color)
    ax1.tick_params(axis='y', labelcolor=color)
    plt.axes(yscale='log')

    
    ax2 = ax1.twinx()  # instantiate a second axes that shares the same x-axis
    color = 'tab:blue'
    ax2.set_ylabel(name2, color=color)  # we already handled the x-label with ax1
    df_2 = df[df['Screen Name']==name2]
    ax2.plot(df_2['As Of Date'],df_2[col], color=color)
    ax2.tick_params(axis='y', labelcolor=color)
    plt.axes(yscale='log')

    fig.tight_layout()  # otherwise the right y-label is slightly clipped
    plt.show()
    
def compare_col_date(df,col,date,name1,name2):
    #input: dataframe, usernames to compare (printed in describe_data)
    #example: compare_likes(data,'jackinthebox','burgerking')
    fig, ax1 = plt.subplots()
    fig.suptitle('Twitter Followers Over Time')

    color = 'tab:red'
    ax1.set_xlabel('Time (months)')
    ax1.set_ylabel(name1, color=color)
    df_1 = df[df['Screen Name']==name1]
    df_1 = df_1[df_1['As Of Date'] <= date]
    ax1.plot(df_1['As Of Date'],df_1[col], color=color)
    ax1.tick_params(axis='y', labelcolor=color)
    ax1.ticklabel_format(axis='y',style='sci')
    
    ax2 = ax1.twinx()  # instantiate a second axes that shares the same x-axis
    color = 'tab:blue'
    ax2.set_ylabel(name2, color=color)  # we already handled the x-label with ax1
    df_2 = df[df['Screen Name']==name2]
    df_2 = df_2[df_2['As Of Date'] <= date]
    ax2.plot(df_2['As Of Date'],df_2[col], color=color)
    ax2.tick_params(axis='y', labelcolor=color)
    fig.tight_layout()  # otherwise the right y-label is slightly clipped
    plt.show()

