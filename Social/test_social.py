# -*- coding: utf-8 -*-
"""
Created on Tue Jun 05 09:51:38 2018

@author: zsaldanh
"""
import pandas as pd
import operator
import json
import numpy as np
import thinknum_pull
import seaborn as sns
import glob
from os.path import normpath, basename
import datetime
dataset_name = 'social_facebook_app'

form_data = {
    'request': json.dumps(
        {
            'tickers': [],
        }
    ),
    'start': 1,
    'limit': 1000,
}

data = thinknum_pull._get_data_single_loop(dataset_name, form_data)
data = data.drop_duplicates()
data.at[820,'Category'] = 'Lifestyle'
cat_data = data.sort_values(['Category','Daily Active Users'], ascending=[False,False])
col = ['Name','Daily Active Users','Weekly Active Users','Monthly Active Users','Category']
cat_data = cat_data[col]
index = cat_data['Category']==''
cat_data.at[index,'Category'] = 'Not Specified'
#top 5 apps in each category by daily active users
util_top = cat_data[cat_data['Category']=='Utilities'].head(9)
life_top = cat_data[cat_data['Category']=='Lifestyle'].head(10)
games_top = cat_data[cat_data['Category']=='Games'].head(6)
ent_top = cat_data[cat_data['Category']=='Entertainment'].head(17)
bus_top = cat_data[cat_data['Category']=='Business'].head(5)
sport_top = cat_data[cat_data['Category']=='Sports'].head(5)
na_top = cat_data[cat_data['Category']=='Not Specified'].head(5)
frames = [util_top,life_top,games_top,ent_top,bus_top,sport_top,na_top]
daily_top5 = pd.concat(frames)
#plot daily active users by category
#g=sns.factorplot(x="Name",y="Daily Active Users", col='Category',kind='bar',data=daily_top5,
 #                size=8,aspect=2)
#g.set_xticklabels(rotation=50)

#weekly vs. monthly (week/month)

g=sns.plot