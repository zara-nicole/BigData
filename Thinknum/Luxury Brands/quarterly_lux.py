# -*- coding: utf-8 -*-
"""
Created on Wed Aug 15 10:28:08 2018

@author: zsaldanh
"""

from sklearn.metrics import mean_squared_error, r2_score
import pandas as pd
import lux_brands as lb
import pandas as pd
import matplotlib.pyplot as plt
from datetime import timedelta  
from sklearn.linear_model import LinearRegression

df = pd.read_excel('KER.xlsx').iloc[:15]
df = df.T.reset_index()
df.columns = df.iloc[0]
df = df.drop([0,1]).reset_index()
df.drop(df.columns[[0,1,2]], axis=1, inplace=True)
df['Quarter'],df['Year'] = df.iloc[:,0].str.split(' ', 1).str
cols = list(df.columns.values) #Make a list of all of the columns in the df
cols.pop(cols.index('Quarter')) #Remove b from list
cols.pop(cols.index('Year')) #Remove x from list
df = df[['Quarter','Year']+cols] 
df.drop(df.columns[[2]], axis=1, inplace=True)
df['3 Months Ending'] = pd.to_datetime(df['3 Months Ending'],format = "%m/%d/%Y")
df['3 Months Ending'] = df['3 Months Ending'] + timedelta(days=1)  
df = df.drop_duplicates()
df_social = lb.combo_data('epa:ker','gucci','GUCCI')

date = df_combo[['As Of Date']]
rev = df_combo[['Revenue']]
likes = df_combo[['Likes']]
followers = df_combo[['Followers']]
lr = LinearRegression()



days=0
while(days < 25):
     df_combo = pd.merge(df,df_social,left_on=['3 Months Ending'],right_on=['As Of Date'] ,how='inner')
     date = df_combo[['As Of Date']]
     rev = df_combo[['Revenue']]
     likes = df_combo[['Likes']]
     followers = df_combo[['Followers']]
     
     slope = lr.fit(likes, rev)
     y_pred = lr.predict(likes)
     r2score = r2_score(rev, y_pred)
     print(r2score)
     
     slope = lr.fit(followers, rev)
     y_pred = lr.predict(followers)
     r2score = r2_score(rev, y_pred)
     print(r2score)
     
     df_social['As Of Date'] = df_social['As Of Date'] + timedelta(days=-1)
     days+=1
     print(days)