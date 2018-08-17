import pandas as pd
import json
import thinknum_pull
dataset_name = 'social_twitter'

#GET ALL THE COMPANIES COVERED WITH MIX ADDED & MAX UPDATED DATE
ticker = "nyse:chs"
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

items_df = thinknum_pull._get_data_single_loop(dataset_name, form_data)
items_df["YMD"] = items_df["As Of Date"].apply(lambda x: x.encode('ascii','ignore')[:8])
items_df["YMD"] = pd.to_datetime(items_df["YMD"])
items_df['MONTH'] = items_df.YMD.dt.strftime("%Y-%m")
items_df = items_df.sort_values(by=['YMD'], ascending = True, kind = 'mergesort')


items_df.to_csv('Z:/Thinknum/output/social_twitter/chs.csv', index = False)

#GET DAILY AGG JOB COUNT BY TICKER
form_data = {
    "request": json.dumps({
                 "tickers": [],
                 "groups": [
                   {
                     "column": "dataset__entity__entity_ticker__ticker__ticker"
                   },    
                   {
                     "column": "screen_name"
                   }    
                 ],
                 "aggregations": [
                   {
                     "column": "time",
                     "type": "min"
                   },
                   {
                     "column": "time",
                     "type": "max"
                   },
                 ]
               }),
        'start': 1,
        'limit': 1000,
}

items_df = thinknum_pull._get_data_multi_loop(dataset_name, form_data) 

items_df.to_csv('Z:/Thinknum/output/job_listings/daily_agg_job_count_by_ticker.csv', index = False)
items_df["YMD"] = items_df["As Of Date"].apply(lambda x: x.encode('ascii','ignore')[:8])
items_df["YMD"] = pd.to_datetime(items_df["YMD"])
items_df['MONTH'] = items_df.YMD.dt.strftime("%Y-%m")
items_df = items_df.sort_values(by=['YMD'], ascending = True, kind = 'mergesort')

#along the history
print "along the history company covered: ", items_df['Ticker Symbol'].nunique()

#Daily total number of active jobs for all companies in Thinknum
daily_active_listing = items_df.groupby('YMD').agg({'Listing ID (Count)': np.sum, 'Ticker Symbol': 'nunique'}).rename(columns={'Listing ID (Count)': 'active_listings', 'Ticker Symbol': 'unique_companies'})
daily_active_listing.to_csv('Z:/Thinknum/output/job_listings/daily_agg_job_count.csv', index = True)

#Monthly total number of active jobs for all companies in Thinknum
monthly_active_listing = items_df.groupby('MONTH').agg({'Listing ID (Count)': np.sum, 'Ticker Symbol': 'nunique'}).rename(columns={'Listing ID (Count)': 'active_listings', 'Ticker Symbol': 'unique_companies'})
monthly_active_listing.to_csv('Z:/Thinknum/output/job_listings/monthly_agg_job_count.csv', index = True)

#What's the coverage of Thinknum job listing dataset
ticker_coverage = pd.read_csv('Z:/Thinknum/output/job_listings/company_info_tb.csv')
ticker_coverage["max_YMD"] = pd.to_datetime(ticker_coverage["As Of Date (Max)"].apply(lambda x: str(x)[:8]))
ticker_coverage["min_YMD"] = pd.to_datetime(ticker_coverage["As Of Date (Min)"].apply(lambda x: str(x)[:8]))

#from Factset sample API get mapping table 
ticker_mapping_tb = pd.read_csv('Z:/Thinknum/output/job_listings/ticker_mmapping_tb.csv')

overlap_df = pd.merge(ticker_mapping_tb, ticker_coverage, on = 'Ticker Symbol', how = 'left')
overlap_df = overlap_df.drop_duplicates()
overlap_df.to_csv('Z:/Thinknum/output/job_listings/company_info_tb_final.csv', index= False)

#GET THE FINAL LIST COERVED IN THINKNUM
ticker_list = pd.read_csv('Z:/Thinknum/output/job_listings/company_info_tb_final.csv')
ticker_list = ticker_list[pd.notnull(ticker_list.max_YMD)]
ticker_list = ticker_list["Ticker Symbol"].tolist() 

for ticker in ticker_list:
    try:
        form_data = {
            "request": json.dumps({
                         "tickers": [ticker],
                         "filters": [
                       {
                         "column": "as_of_date",
                         "type": ">=",
                         "value": ["20160801"]
                       }],
                }
                ),                    
                'start': 1,
                'limit': 1000,
        }
        items_df = thinknum_pull._get_data_multi_loop(dataset_name, form_data)
        items_df.to_csv('Z:/Thinknum/output/job_listings/target_company/{}.csv'.format(ticker.split(':')[1]), index = False, encoding  = 'utf-8')

    except Exception as ex:
        print 'Error encountered during accessing ticker:', ticker
        pass

#GET THE FINAL WORKING LIST
import glob
from os.path import normpath, basename
ticker_list_temp =  glob.glob("Z:/Thinknum/output/job_listings/target_company/*.csv")
ticker_list_temp = [basename(normpath(i))[:-4] for i in ticker_list_temp]
ticker_list_wip = [i for i in ticker_list if i.split(":")[1] in ticker_list_temp]

###################
#RESTURANTS 
#####################
res_list = 'DPZ-US,DRI-US,CMG-US,BWLD-US,DNKN-US,WEN-US,CBRL-US,TXRH-US,PZZA-US,JACK-US,CAKE-US,SHAK-US,WING-US,BJRI-US,BOJA-US,EAT-US,PLAY-US,TACO-US,DIN-US,LOCO-US,FRGI-US,HABT-US,MCD-US,NDLS-US,BLMN-US,FRSH-US,PBPB-US,RRGB-US,SONC-US,SBUX-US,ZOES-US,QSR-US'.split(",")
res_list = [x[:-3] for x in res_list]

#get prepared ticker
res_list = pd.read_csv('Z:/Thinknum/output/job_listings/res_ticker_mmapping_tb.csv')
res_list = res_list['Ticker Symbol'].tolist()

for ticker in res_list:
    try:
        form_data = {
            "request": json.dumps({
                         "tickers": [ticker],
                         "filters": [
                       {
                         "column": "as_of_date",
                         "type": ">=",
                         "value": ["20160801"]
                       }],
                }
                ),                    
                'start': 1,
                'limit': 1000,
        }
        items_df = thinknum_pull._get_data_multi_loop(dataset_name, form_data)
        items_df.to_csv('Z:/Thinknum/output/job_listings/res_company/{}.csv'.format(ticker.split(':')[1]), index = False, encoding  = 'utf-8')
        break
    except Exception as ex:
        print 'Error encountered during accessing ticker:', ticker
        pass


########################
###
#######################