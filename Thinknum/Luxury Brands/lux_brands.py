# -*- coding: utf-8 -*-
"""
Created on Wed Aug 08 09:11:24 2018

@author: zsaldanh
"""


import pandas as pd
import json
import thinknum_pull
import pandas as pd
import matplotlib.pyplot as plt

import matplotlib.pyplot as plt
from matplotlib.ticker import ScalarFormatter, FormatStrFormatter
import seaborn as sns

# lux_brands = ['epa:ker','epa:mc','hk:1913','private:christianlouboutin']
# df_twitter = multi_tickers('social_twitter',lux_brands)
# df_facebook = multi_tickers('social_facebook',lux_brands)
def multi_tickers(dataset_name,dict_names):
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

class FixedOrderFormatter(ScalarFormatter):
    """Formats axis ticks using scientific notation with a constant order of 
    magnitude"""
    def __init__(self, order_of_mag=0, useOffset=True, useMathText=False):
        self._order_of_mag = order_of_mag
        ScalarFormatter.__init__(self, useOffset=useOffset, 
                                 useMathText=useMathText)
    def _set_orderOfMagnitude(self, range):
        """Over-riding this to avoid having orderOfMagnitude reset elsewhere"""
        self.orderOfMagnitude = self._order_of_mag
        
def fb_followers(df,col,name):
    #input: dataframe, usernames to compare (printed in describe_data)
    #example: compare_likes(data,'jackinthebox','burgerking')
    sns.set(style="ticks")
    fig, ax1 = plt.subplots(figsize=(12,8))
    fig.suptitle('Facebook Likes Over Time',y=1)

    color = 'tab:red'
    ax1.set_xlabel('Time (months)',labelpad=15,fontsize=15)
    ax1.set_ylabel(name, color=color,labelpad=15,fontsize=15)

    ax1.plot(df['As Of Date'],df[col], color=color)
    ax1.tick_params(axis='y', labelcolor=color,labelsize=11)
    ax1.tick_params(axis='x', labelsize=11)
    ax1.ticklabel_format(axis='y',style='sci')
    ax1.yaxis.set_major_formatter(FixedOrderFormatter(6))
    
    fig.tight_layout()  # otherwise the right y-label is slightly clipped
    plt.show()
    
def twitter_followers(df,col,name):
    #input: dataframe, usernames to compare (printed in describe_data)
    #example: compare_likes(data,'jackinthebox','burgerking')
    sns.set(style="ticks")
    fig, ax1 = plt.subplots(figsize=(12,8))
    fig.suptitle('Twitter Followers Over Time',y=1)

    color = 'tab:red'
    ax1.set_xlabel('Time (months)',labelpad=15,fontsize=15)
    ax1.set_ylabel(name, color=color,labelpad=15,fontsize=15)

    ax1.plot(df['As Of Date'],df[col], color=color)
    ax1.tick_params(axis='y', labelcolor=color,labelsize=11)
    ax1.tick_params(axis='x', labelsize=11)
    ax1.ticklabel_format(axis='y',style='sci')
    ax1.yaxis.set_major_formatter(FixedOrderFormatter(6))
    
    fig.tight_layout()  # otherwise the right y-label is slightly clipped
    plt.show()
    
def weight_avg(df):
    #input: dataframe, usernames to compare (printed in describe_data)
    #example: compare_likes(data,'jackinthebox','burgerking')
    sns.set(style="ticks")
    fig, ax1 = plt.subplots(figsize=(12,8))
    fig.suptitle('Weighted Average of FB Likes and Twitter Followers',y=1)
    name = df.loc[1,'Username_x']
    color = 'tab:red'
    ax1.set_xlabel('Time (months)',labelpad=15,fontsize=15)
    ax1.set_ylabel(name, color=color,labelpad=15,fontsize=15)
    ax1.plot(df['As Of Date'],df['Weighted Average'], color=color)
    ax1.tick_params(axis='y', labelcolor=color,labelsize=11)
    ax1.tick_params(axis='x', labelsize=11)
    ax1.ticklabel_format(axis='y',style='sci')
    ax1.yaxis.set_major_formatter(FixedOrderFormatter(6))
    
    fig.tight_layout()  # otherwise the right y-label is slightly clipped
    plt.show()
    
def choose_graph(df):
    #check if its fb data or twitter data
    #calculate shift and std for fb df and twitter df
    #find out which one has more amount of outliers
    #return the graph of one with less outlier
   name = df.loc[1,'Username_x']
   fb = None
   tw = None
   df['Shift_FB'] = pd.Series(df['Likes'].shift(1),index=df.index)
   df['Diff_FB'] = pd.Series((df['Shift_FB']-df['Likes']),index=df.index)        
   
   df['Shift_TW'] = pd.Series(df['Followers'].shift(1),index=df.index)
   df['Diff_TW'] = pd.Series((df['Shift_TW']-df['Followers']),index=df.index)
   
   std_ = df.std();
   fb_std = std_['Diff_FB']
   tw_std = std_['Diff_TW']
   
   fb_mean = df['Diff_FB'].mean()
   upper_lim = fb_mean + 3*fb_std
   lower_lim = fb_mean + (-3*fb_std)
   print("upper limit fb:")
   print(upper_lim)
   print("lower limit fb:")
   print(lower_lim)

   if (len(df[df['Diff_FB'] >= upper_lim].index) > 0 or len(df[df['Diff_FB'] <= lower_lim].index) > 0):
       fb = True;
   else:
       fb = False;
   fb_size = len(df[df['Diff_FB'] >= upper_lim].index) + len(df[df['Diff_FB'] <= lower_lim].index)
   print("fb outliers")
   print(df[df['Diff_FB'] >= upper_lim])
   print(df[df['Diff_FB'] <= lower_lim])
            
   tw_mean = df['Diff_TW'].mean()
   upper_lim = tw_mean + 3*tw_std
   lower_lim = tw_mean + (-3*tw_std)
   print("upper limit tw:")
   print(upper_lim)
   print("lower limit tw:")
   print(lower_lim)

   if (len(df[df['Diff_TW'] >= upper_lim].index) > 0 or len(df[df['Diff_TW'] <= lower_lim].index) > 0):
       tw = True;
   else:
       tw = False;
   tw_size = len(df[df['Diff_TW'] >= upper_lim].index) + len(df[df['Diff_TW'] <= lower_lim].index)
   print("twitter outliers")
   print(df[df['Diff_TW'] >= upper_lim])
   print(df[df['Diff_TW'] <= lower_lim])
   print("tw size")
   print(tw_size)
   print("fb size")
   print(fb_size)
   
   #plot diff column
   diff_col(df,'Diff_FB','Facebook Diff');
   diff_col(df,'Diff_TW','Twitter Diff');
   
   if(tw==True and fb==False):
       fb_followers(df,'Likes',name);
   elif(tw==False and fb==True):
       twitter_followers(df,'Followers',name);
   else:
       if(tw_size > fb_size):
           fb_followers(df,'Likes',name);
       elif(tw_size < fb_size):      
           twitter_followers(df,'Followers',name);
       else:
           fb_followers(df,'Likes',name);

    
def combo_data(ticker,screen_name, user_name):
   df_twitter = multi_tickers('social_twitter',ticker)
   df_twitter = df_twitter[['Ticker Symbol','As Of Date','Screen Name','Followers']]
   df_twitter.columns=['Ticker Symbol','As Of Date','Username','Followers']
   
   df_facebook = multi_tickers('social_facebook',ticker)
   df_facebook = df_facebook[['Ticker Symbol','As Of Date','Username','Likes']]
   
   df_twitter = df_twitter[df_twitter['Username'] == screen_name]
   df_facebook = df_facebook[df_facebook['Username'] == user_name]

   df_combo = pd.merge(df_twitter,df_facebook,on=['As Of Date'],how='inner')
   return df_combo
   
def calc_weight(df):
   df['Weighted Likes'] = pd.Series(df['Likes']/(df['Likes']+df['Followers']),index=df.index)
   df['Weighted Followers'] = pd.Series(df['Followers']/(df['Likes']+df['Followers']),index=df.index)
   df['Weighted Average'] = pd.Series((df['Weighted Likes']*df['Likes']+df['Followers']*df['Weighted Followers']),index=df.index)
   
   return df

def calc_shift(df):
   df['Shift'] = pd.Series(df['Weighted Average'].shift(1),index=df.index)
   df['Diff'] = pd.Series((df['Shift']-df['Weighted Average']),index=df.index)
    
   return df

def calc_std(df):
    std_combo = df.std();
    diff_std = std_combo['Diff']
    mean = df['Diff'].mean()
    upper_lim = mean + 3*diff_std
    lower_lim = mean + (-3*diff_std)
    if (df[df['Diff'] >= upper_lim].size > 0 or df[df['Diff'] <= lower_lim].size > 0):
        choose_graph(df);
    else:
        weight_avg(df);
        
def entire(ticker,screenname,username):
    #screenname = twitter
    #username = facebook
    #example for Louis Vuitton: entire('epa:mc','LouisVuitton','LouisVuitton')
    #entire('epa:ker','gucci','GUCCI')
    df = combo_data(ticker,screenname,username);
    df = calc_weight(df);
    df = calc_shift(df);

    return calc_std(df);

def diff_col(df,col,name):
    sns.set(style="ticks")
    fig, ax1 = plt.subplots(figsize=(12,8))
    fig.suptitle('Difference',y=1)

    color = 'tab:red'
    ax1.set_xlabel('Time (months)',labelpad=15,fontsize=15)
    ax1.set_ylabel(name, color=color,labelpad=15,fontsize=15)

    ax1.plot(df['As Of Date'],df[col], color=color)
    ax1.tick_params(axis='y', labelcolor=color,labelsize=11)
    ax1.tick_params(axis='x', labelsize=11)
    ax1.ticklabel_format(axis='y',style='sci')
    ax1.yaxis.set_major_formatter(FixedOrderFormatter(6))
    
    fig.tight_layout()  # otherwise the right y-label is slightly clipped
    plt.show()
        