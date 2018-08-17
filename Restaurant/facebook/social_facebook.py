import pandas as pd
import json
import thinknum_pull
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.ticker import ScalarFormatter, FormatStrFormatter

dataset_name = 'social_facebook'

#GET ALL THE COMPANIES COVERED WITH MIX ADDED & MAX UPDATED DATE
def all_ticker():    
    form_data = {
        "request": json.dumps({
                     "tickers": [],
                    }),
            'start': 1,
            'limit': 1000,
    }    
    
    items_df = thinknum_pull._get_data_multi_loop(dataset_name, form_data)
    items_df['As Of Date'] = pd.to_datetime(items_df['As Of Date'],format = "%Y-%m-%d %H:%M:%S")
    return items_df

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
    #items_df["YMD"] = items_df["As Of Date"].apply(lambda x: x.encode('ascii','ignore')[:8])
    #items_df["YMD"] = pd.to_datetime(items_df["YMD"])
    #items_df['MONTH'] = items_df.YMD.dt.strftime("%Y-%m")
    items_df['As Of Date'] = pd.to_datetime(items_df['As Of Date'],format = "%Y-%m-%d %H:%M:%S")
    #items_df['MONTH'] = pd.to_datetime(items_df['MONTH'])
    #items_df = items_df.sort_values(by=['YMD'], ascending = True, kind = 'mergesort')
    return items_df

def multi_tickers(list_names):
    ticker = list_names
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

def compare_col(df,col,name1,name2):
    #input: dataframe, usernames to compare (printed in describe_data)
    #example: compare_col(data,'Followers','jackinthebox','burgerking')
    sns.set(style="ticks")
    fig, ax1 = plt.subplots()
    fig.suptitle('Facebook Followers Over Time')

    color = 'tab:red'
    ax1.set_xlabel('Time (months)',labelpad=15,fontsize=15)
    ax1.set_ylabel(name1, color=color, labelpad=15,fontsize=15)
    df_1 = df[df['Username']==name1]
    ax1.plot(df_1['As Of Date'],df_1[col], color=color)
    ax1.tick_params(axis='y', labelcolor=color)
    ax1.yaxis.set_major_formatter(FixedOrderFormatter(3))
    #plt.axes(yscale='log')

    
    ax2 = ax1.twinx()  # instantiate a second axes that shares the same x-axis
    color = 'tab:blue'
    ax2.set_ylabel(name2, color=color, labelpad=15,fontsize=15)  # we already handled the x-label with ax1
    df_2 = df[df['Username']==name2]
    ax2.plot(df_2['As Of Date'],df_2[col], color=color)
    ax2.tick_params(axis='y', labelcolor=color)
    ax2.yaxis.set_major_formatter(FixedOrderFormatter(3))
    #plt.axes(yscale='log')

    fig.tight_layout()  # otherwise the right y-label is slightly clipped
    plt.show()
    
def compare_col_date(df,col,date,name1,name2):
    #input: dataframe, usernames to compare (printed in describe_data)
    #example: compare_likes(data,'jackinthebox','burgerking')
    fig, ax1 = plt.subplots(figsize=(12,8))
    fig.suptitle('Facebook Likes (millions) Over Time',y=1,fontsize=15)

    color = 'tab:red'
    ax1.set_xlabel('Time (months)',labelpad=15,fontsize=15)
    ax1.set_ylabel('McDonalds', color=color,labelpad=15,fontsize=15)
    df_1 = df[df['Username']==name1]
    df_1 = df_1[df_1['As Of Date'] >= date]
    ax1.plot(df_1['As Of Date'],df_1[col], color=color)
    ax1.tick_params(axis='y', labelcolor=color)
    ax1.yaxis.set_major_formatter(FixedOrderFormatter(6))
    #ax1.ticklabel_format(axis='y',style='sci')
    
    ax2 = ax1.twinx()  # instantiate a second axes that shares the same x-axis
    color = 'tab:blue'
    ax2.set_ylabel('Burger King', color=color, labelpad=15,fontsize=15)  # we already handled the x-label with ax1
    df_2 = df[df['Username']==name2]
    df_2 = df_2[df_2['As Of Date'] >= date]
    ax2.plot(df_2['As Of Date'],df_2[col], color=color)
    ax2.tick_params(axis='y', labelcolor=color)
    ax2.yaxis.set_major_formatter(FixedOrderFormatter(6))
    fig.tight_layout()  # otherwise the right y-label is slightly clipped
    plt.show()
