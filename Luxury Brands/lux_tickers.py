# -*- coding: utf-8 -*-
"""
Created on Tue Jul 31 16:43:35 2018

@author: zsaldanh
"""

import pandas as pd
import json
import thinknum_pull
import pandas as pd
import matplotlib.pyplot as plt

dataset_name = 'social_twitter'

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

