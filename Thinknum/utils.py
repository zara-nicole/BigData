import json
import requests
import os
import pandas as pd

os.environ["HTTP_PROXY"] = "twdevproxy.nb.com:80"
os.environ["HTTPS_PROXY"] = "twdevproxy.nb.com:80"

### STEP 1: Authorization
# Setup and send request to get an authorization token
def _setup_auth():
    api_host = 'https://data.thinknum.com'
    api_version = '20151130'
    api_client_id = 'a4ed06514071d78fb3c1'
    api_client_secret = '3472aea69e16c1726b571a6f1d327a03f9f0b2db'  # THIS KEY SHOULD NEVER BE PUBLICALLY ACCESSIBLE
    cert_file = '\\\\nb.com\\corp\Apps\\BigData\\certificates\\ca-bundle.crt'

    payload = {
        'version': api_version,
        'client_id': api_client_id,
        'client_secret': api_client_secret
    }
    request_url = api_host + '/api/authorize'
    r = requests.post(request_url, data=payload, verify=cert_file)
    
    if r.status_code != 200:
        raise Exception('Failed to authorize: ' + r.text)
    
    token_data = json.loads(r.text)
    
    api_auth_token = token_data['auth_token']
    api_auth_expires = token_data['auth_expires']
    api_auth_headers = {
        "X-API-Version": api_version,
        "Authorization": "token {token}".format(token=api_auth_token)
    }
    
    print ('Authorization Token:', api_auth_token, 'Expires:', api_auth_expires)
    return api_host, cert_file, api_auth_headers

api_host, cert_file, api_auth_headers =  _setup_auth()
### STEP 2: Sample POST endpoint
# Queries for a specific dataset
def single_loop(dataset_name, form_data):
    dataset_name = 'job_listings'
    
    form_data = {
        'request': json.dumps(
            {
                'tickers': [],
            }
        ),
        'start': 1,
        'limit': 1000
    }
    
    request_url = api_host + '/connections/dataset/' + dataset_name + '/query/new'
    r = requests.post(request_url, headers=api_auth_headers, data=form_data, verify=cert_file)
    
    if r.status_code != 200:
        raise Exception('Failed to POST: ' + r.text)
    
    dataset_data = json.loads(r.text)
    items = pd.DataFrame.from_dict(dataset_data['items']['rows'])
    items.columns = pd.DataFrame.from_dict(dataset_data['items']['fields'])['display_name'].tolist()
    return items


############################
#STEP2 test across all datasets
#############################
list_df = pd.read_csv('Z:/Thinknum/output/thinknum_datasets_list.csv')
ids_ = list_df['id'].tolist()
for i in ids_:
    dataset_name = i
    
    form_data = {
        'request': json.dumps(
            {
                'tickers': ['nasdaq:aapl'],
            }
        ),
        'start': 1,
        'limit': 10,
    }
    
    request_url = api_host + '/connections/dataset/' + 'store' + '/query/new'
    
    r = requests.post(request_url, headers=api_auth_headers, data=form_data, verify=cert_file)
    if r.status_code != 200:
        raise Exception('Failed to POST: ' + r.text)
    dataset_data = json.loads(r.text)
    #get dataset_fields
    dataset_fields = pd.DataFrame.from_dict(dataset_data['dataset_fields'])
    dataset_fields.to_csv('Z:/Thinknum/output/jumpshot_datafields/{}_dataset_fields_no_detailed_desp.csv'.format(dataset_name, dataset_name), index = False)
    print dataset_name

### STEP 3: Sample POST endpoint- Store
# Queries for a specific dataset

dataset_name = 'store'
cont_flag = "Y"
start_ = 1
items_list = []
while cont_flag == "Y":
    form_data = {
        'request': json.dumps(
            {
                'tickers': ['nasdaq:tsco'],
            }
        ),
        'start': start_,
        'limit': 1000,
    }
    
    request_url = api_host + '/connections/dataset/' + dataset_name + '/query/new'
    r = requests.post(request_url, headers=api_auth_headers, data=form_data, verify=cert_file)
    
    if r.status_code != 200:
        raise Exception('Failed to POST: ' + r.text)
    
    dataset_data = json.loads(r.text)
    items = pd.DataFrame.from_dict(dataset_data['items']['rows'])
    items.columns = pd.DataFrame.from_dict(dataset_data['items']['fields'])['display_name'].tolist()
    items_list.append(items)
    #check more data or not
    if dataset_data['more']:
        start_ += 1000
        continue
    else:
        break
items_df = pd.concat(items_list)
items_df.to_csv('Z:/Thinknum/output/{}/tsco.csv'.format(dataset_name), index = False, encoding='utf-8')

###Facebook data - users use FB credentials to log on
"""
Track users logging into a website or app via their FB login credentials.
why DAU is constant round numbers
"""
dataset_name = 'social_facebook_app'
cont_flag = "Y"
start_ = 1
items_list = []
while cont_flag == "Y":

    form_data = {
        "request": json.dumps({
                     "tickers": [],
                     "groups": [
                       {
                         "column": "time"
                       },    
                     ],
                     "aggregations": [
                       {
                         "column": "daily_active_users",
                         "type": "sum"
                       },
                       {
                         "column": "weekly_active_users",
                         "type": "sum"
                       },
                       {
                         "column": "monthly_active_users",
                         "type": "sum"
                       },
                       {
                         "column": "app_id",
                         "type": "count"
                       },
                       {
                         "column": "id",
                         "type": "count"
                       },
                       {
                         "column": "name",
                         "type": "count"
                       }
                     ]
                   }),
            'start': start_,
            'limit': 1000,
    }
    
    request_url = api_host + '/connections/dataset/' + dataset_name + '/query/new'
    r = requests.post(request_url, headers=api_auth_headers, data=form_data, verify=cert_file)
    
    if r.status_code != 200:
        raise Exception('Failed to POST: ' + r.text)
    
    dataset_data = json.loads(r.text)
    items = pd.DataFrame.from_dict(dataset_data['items']['rows'])
    items.columns = pd.DataFrame.from_dict(dataset_data['items']['fields'])['display_name'].tolist()
    items_list.append(items)
    #check more data or not
    if dataset_data['more']:
        start_ += 1000
        continue
    else:
        break
items_df = pd.concat(items_list)
items_df["YMD"] = items_df["As Of Date"].apply(lambda x: x.encode('ascii','ignore')[:8])
items_df["YMD"] = pd.to_datetime(items_df["YMD"])
items_df['MONTH'] = items_df.YMD.dt.strftime("%Y-%m")
items_df = items_df.sort_values(by=['YMD'], ascending = True, kind = 'mergesort')
items_df.to_csv('Z:/Thinknum/output/{}/FB_DAU.csv'.format(dataset_name), index = False, encoding='utf-8')

###Facebook data - users use FB followers
"""
confirm after 2016/02/09, dates are continuous
"""
dataset_name = 'social_facebook'
cont_flag = "Y"
start_ = 1
items_list = []
while cont_flag == "Y":
    form_data = {
        "request": json.dumps({
                     "tickers": [],
                     "groups": [
                       {
                         "column": "time"
                       },    
                     ],
                     "aggregations": [
                       {
                         "column": "likes",
                         "type": "sum"
                       },
                       {
                         "column": "checkins",
                         "type": "sum"
                       },
                       {
                         "column": "were_here_count",
                         "type": "sum"
                       },
                       {
                         "column": "talking_about_count",
                         "type": "sum"
                       },
                       {
                         "column": "id",
                         "type": "count"
                       },
                       {
                         "column": "facebook_id",
                         "type": "count"
                       }
                     ]
                   }),
            'start': start_,
            'limit': 1000,
    }

    request_url = api_host + '/connections/dataset/' + dataset_name + '/query/new'
    r = requests.post(request_url, headers=api_auth_headers, data=form_data, verify=cert_file)
    
    if r.status_code != 200:
        raise Exception('Failed to POST: ' + r.text)
    
    dataset_data = json.loads(r.text)
    items = pd.DataFrame.from_dict(dataset_data['items']['rows'])
    items.columns = pd.DataFrame.from_dict(dataset_data['items']['fields'])['display_name'].tolist()
    items_list.append(items)
    #check more data or not
    if dataset_data['more']:
        start_ += 1000
        print start_
        continue
    else:
        break
items_df = pd.concat(items_list)
items_df["YMD"] = items_df["As Of Date"].apply(lambda x: x.encode('ascii','ignore')[:8])
items_df["YMD"] = pd.to_datetime(items_df["YMD"])
items_df['MONTH'] = items_df.YMD.dt.strftime("%Y-%m")
items_df = items_df.sort_values(by=['YMD'], ascending = True, kind = 'mergesort')

items_df.to_csv('Z:/Thinknum/output/{}/FB.csv'.format(dataset_name), index = False, encoding='utf-8')

#rename columns
#items_df = items_df.rename(columns = {'Likes (Sum)':'Total_Likes'}, inplace = True)
#add columns - likes per facebook profile
items_df["Likes per Profile"] = items_df["Likes (Sum)"]*1.0/items_df["Facebook Id (Count)"]
items_df["Checkins per Profile"] = items_df["Checkins (Sum)"]*1.0/items_df["Facebook Id (Count)"]


#not groupby level
dataset_name = 'job_listings'
cont_flag = "Y"
start_ = 1
items_list = []
while cont_flag == "Y":
#    form_data = {
#        "request": json.dumps({
#                     "tickers": [],
#                     "groups": [
#                       {
#                         "column": "as_of_date"
#                       },    
#                       {
#                         "column": "dataset__entity__entity_ticker__ticker__ticker"
#                       }    
#                     ],
#                     "aggregations": [
#                       {
#                         "column": "listing_id",
#                         "type": "count"
#                       },
#                     ]
#                   }),
#            'start': start_,
#            'limit': 1000,
#    }
    form_data = {
        "request": json.dumps({
                     "tickers": [],
                     "groups": [
                       {
                         "column": "dataset__entity__entity_ticker__ticker__ticker"
                       },      
                     ],
                     "aggregations": [
                       {
                         "column": "as_of_date",
                         "type": "min"
                       },
                       {
                         "column": "as_of_date",
                         "type": "max"
                       },
                     ]
                   }),
            'start': start_,
            'limit': 1000,
    }    
    request_url = api_host + '/connections/dataset/' + dataset_name + '/query/new'
    r = requests.post(request_url, headers=api_auth_headers, data=form_data, verify=cert_file)
    
    if r.status_code != 200:
        raise Exception('Failed to POST: ' + r.text)
    
    dataset_data = json.loads(r.text)
    items = pd.DataFrame.from_dict(dataset_data['items']['rows'])
    items.columns = pd.DataFrame.from_dict(dataset_data['items']['fields'])['display_name'].tolist()
    items_list.append(items)
#    if start_ == 1:
#        #get dataset_fields
#        dataset_fields = pd.DataFrame.from_dict(dataset_data['dataset_fields'])
#        dataset_fields.to_csv('Z:/Thinknum/output/thinknum_datafields/{}_datafields.csv'.format(dataset_name), index = False)
    
    
    #check more data or not
    if dataset_data['more']:
        start_ += 1000
        print start_
        continue
    else:
        break
items_df = pd.concat(items_list)
items_df.to_csv('Z:/Thinknum/output/job_listings/company_info_tb.csv', index = False)

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

#What's coverage
ticker_coverage = pd.read_csv('Z:/Thinknum/output/job_listings/company_info_tb.csv')
ticker_coverage["max_YMD"] = pd.to_datetime(ticker_coverage["As Of Date (Max)"].apply(lambda x: str(x)[:8]))
ticker_coverage["min_YMD"] = pd.to_datetime(ticker_coverage["As Of Date (Min)"].apply(lambda x: str(x)[:8]))

ticker_mapping_tb = pd.read_csv('Z:/Thinknum/output/job_listings/ticker_mmapping_tb.csv')

overlap_df = pd.merge(ticker_mapping_tb, ticker_coverage, on = 'Ticker Symbol', how = 'left')
overlap_df = overlap_df.drop_duplicates()
overlap_df.to_csv('Z:/Thinknum/output/job_listings/company_info_tb_final.csv', index= False)

#get the wokring ticker list
ticker_list = pd.read_csv('Z:/Thinknum/output/job_listings/company_info_tb_final.csv')
ticker_list = ticker_list[pd.notnull(ticker_list.max_YMD)]
ticker_list = ticker_list["Ticker Symbol"].tolist() 

for ticker in ticker_list:
    try:
        dataset_name = 'job_listings'
        print ticker
        cont_flag = "Y"
        start_ = 1
        items_list = []
        while cont_flag == "Y":
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
                    'start': start_,
                    'limit': 1000,
            }
            request_url = api_host + '/connections/dataset/' + dataset_name + '/query/new'
            r = requests.post(request_url, headers=api_auth_headers, data=form_data, verify=cert_file)
            
            if r.status_code != 200:
                raise Exception('Failed to POST: ' + r.text)
            
            dataset_data = json.loads(r.text)
            items = pd.DataFrame.from_dict(dataset_data['items']['rows'])
            items.columns = pd.DataFrame.from_dict(dataset_data['items']['fields'])['display_name'].tolist()
            items_list.append(items)    
            
            #check more data or not
            if dataset_data['more']:
                start_ += 1000
                print start_
                continue
            else:
                break
        items_df = pd.concat(items_list)
        items_df.to_csv('Z:/Thinknum/output/job_listings/target_company/{}.csv'.format(ticker.split(':')[1]), index = False, encoding  = 'utf-8')

    except Exception as ex:
        print 'Error encountered during accessing ticker:', ticker
        pass




#BESTSELLERS
dataset_name = 'bestsellers'
cont_flag = "Y"
start_ = 1
items_list = []
while cont_flag == "Y":
    form_data = {
        "request": json.dumps({
                     "tickers": ['nasdaq:amzn'],
                     "filters": [
                       {
                         "column": "category",
                         "type": "=",
                         "value": ["Health & Household"]
                       },
#                       {
#                         "column": "product_url",
#                         "type": "=",
#                         "value": ["https://www.amazon.com/Best-Sellers-Electronics-Batteries/"]
#                       },
#                       {
#                         "column": "brand",
#                         "type": "=",
#                         "value": ["AmazonBasics", "Anker", "Duracell", "Energizer", "Panasonic"]
#                       },
#                       { 
#                         "column": "likes"            
#                       }
    
                     ],
                     "groups": [
                       {
                         "column": "as_of_date"
                       },
                       {
                         "column": "product_url"
                       },
                       {
                         "column": "name"
                       },
                       {
                         "column": "brand"
                       },
                       {
                         "column": "category_rank"
                       },
                       {
                         "column": "listprice"
                       },
                       {
                         "column": "discount_percent"
                       }
                     ],
                     "aggregations": [
                       {
                         "column": "id",
                         "type": "count"
                       }
                     ]
                   }),
            'start': start_,
            'limit': 1000,
    }
            
#    form_data = {
#        "request": json.dumps({
#                     "tickers": ['nasdaq:amzn'],
#                     "filters": [
#                       {
#                         "column": "category",
#                         "type": "=",
#                         "value": ["Electronics"]
#                       },
#                       {
#                         "column": "brand",
#                         "type": "=",
#                         "value": ["AmazonBasics", "Anker", "Duracell", "Energizer", "Panasonic"]
#                       },
##                       { 
##                         "column": "likes"            
##                       }
#    
#                     ],
#                     "groups": [
#                       {
#                         "column": "as_of_date"
#                       },
#                       {
#                         "column": "brand"
#                       }],
#                     "aggregations": [
#                       {
#                         "column": "brand",
#                         "type": "count"
#                       }
#                     ]
#                   }),
#            'start': start_,
#            'limit': 1000,
#    }
#    form_data = {
#        "request": json.dumps({
#                     "tickers": ['nasdaq:amzn'],
#                     "groups": [
#                       {
#                         "column": "category"
#                       }    
#                     ],
#                     "aggregations": [
#                       {
#                         "column": "brand",
#                         "type": "count"
#                       }
#                     ]
#                   }),
#            'start': start_,
#            'limit': 200,
#    }
    
    request_url = api_host + '/connections/dataset/' + dataset_name + '/query/new'
    r = requests.post(request_url, headers=api_auth_headers, data=form_data, verify=cert_file)
    
    if r.status_code != 200:
        raise Exception('Failed to POST: ' + r.text)
    
    dataset_data = json.loads(r.text)
    items = pd.DataFrame.from_dict(dataset_data['items']['rows'])
    items.columns = pd.DataFrame.from_dict(dataset_data['items']['fields'])['display_name'].tolist()
    items_list.append(items)
#    if start_ == 1:
#        #get dataset_fields
#        dataset_fields = pd.DataFrame.from_dict(dataset_data['dataset_fields'])
#        dataset_fields.to_csv('Z:/Thinknum/output/thinknum_datafields/{}_dataset_fields_no_detailed_desp.csv'.format(dataset_name), index = False)

    #check more data or not
    if dataset_data['more']:
        start_ += 1000
        print start_
        continue
    else:
        break
items_df = pd.concat(items_list)

#after collect all eletronic department, filter out url containing batteries
#lower case
items_df['Name'] = items_df['Name'].apply(lambda x: x.lower().encode('utf-8'))#filter batteies string

pat = r'batteries'
battery_df = items_df[items_df['Name'].str.contains(pat)]
battery_df.shape

#clean up items
#black_pat = r'(batter(y|ies) (included?|operate(s|d)|chargers?|analyzers?)|batter(y|ies) ?\+|(\&|by|for|with|on|incl|include|holds?).*?batter(y|ies))'
black_pat = r"batter(y|ies) (included?|operate(s|d)|chargers?|analyzers?)|batter(y|ies) ?\+|(\&|incl|include|holds?).*?batter(y|ies)| (by|for|with|on|) .*?batter(y|ies)"
battery_clean_df = battery_df[~battery_df.Name.str.contains(black_pat)]

battery_clean_df["Date"] = pd.to_datetime(battery_clean_df["As Of Date"].apply(lambda x: str(x)[:8]))


def _get_brand(x):
    if(pd.notnull(x.loc["Brand"])):
        return x.loc["Brand"]
    elif "amazonbasics" in str(x.loc["Name"]):
        return "AmazonBasics"
    elif "energizer" in str(x.loc["Name"]):
        return "Energizer"
    elif "duracell" in str(x.loc["Name"]):
        return "Duracell"
    else:
        return "Others"
    
battery_clean_df["Adjusted_Brand"] = battery_clean_df.apply(lambda row: _get_brand(row), axis=1)
battery_clean_df['MONTH'] = battery_clean_df["Date"].dt.strftime("%Y-%m")


pivot_tb = battery_clean_df.pivot_table(index = 'Date', columns="Adjusted_Brand", values = "Category Rank", aggfunc = "count")
pivot_tb = pivot_tb.fillna(0.0)
columns_list = list(pivot_tb)
summary_rate = pivot_tb.copy()
summary_rate['Total'] = summary_rate.sum(axis=1)
summary_rate[columns_list] = summary_rate[columns_list].div(summary_rate['Total'], axis = 0)


#backfill daily
#battery_clean_df = battery_clean_df.set_index('Date')
#battery_clean_df.sort_index(ascending = True, kind = 'mergesort',inplace=True)
#battery_clean_df = battery_clean_df.set_index(battery_clean_df.Date).asfreq('D')#.fillna(0.0)
#battery_clean_df.index.rename('Date', inplace=True)

#daily_agg_df= daily_agg_df.set_index('YMD')



battery_clean_df = battery_clean_df["brand"].fillna("others")
battery_clean_df = battery_clean_df.sort_values("Date", ascending = True)
battery_clean_df.to_csv("battery_bestsellers.csv", index = False, encoding  = 'utf-8')
["AmazonBasics", "Anker", "Duracell", "Energizer", "Panasonic"] 

