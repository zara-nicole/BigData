import json
import requests
import os
import pandas as pd
import time
import datetime
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
def _get_data_fields(dataset_name, form_data):

    request_url = api_host + '/connections/dataset/' + 'store' + '/query/new'
    
    r = requests.post(request_url, headers=api_auth_headers, data=form_data, verify=cert_file)
    if r.status_code != 200:
        raise Exception('Failed to POST: ' + r.text)
    dataset_data = json.loads(r.text)
    #get dataset_fields
    dataset_fields = pd.DataFrame.from_dict(dataset_data['dataset_fields'])
    return dataset_fields

dataset_name = "job_listings"

form_data = {
    'request': json.dumps(
        {
            'tickers': [],
        }
    ),
    'start': 1,
    'limit': 1000,
}

def _get_data_single_loop(dataset_name, form_data):
    request_url = api_host + '/connections/dataset/' + dataset_name + '/query/new'
    r = requests.post(request_url, headers=api_auth_headers, data=form_data, verify=cert_file)
    
    if r.status_code != 200:
        raise Exception('Failed to POST: ' + r.text)
    
    dataset_data = json.loads(r.text)
    items = pd.DataFrame.from_dict(dataset_data['items']['rows'])
    items.columns = pd.DataFrame.from_dict(dataset_data['items']['fields'])['display_name'].tolist()
    return items

def _get_data_multi_loop(dataset_name, form_data):
    cont_flag = "Y"
    start_ = form_data["start"]
    items_list = []
    while cont_flag == "Y":
        form_data["start"] =  start_      
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
            print(start_)
            continue
        else:
            break
    items_df = pd.concat(items_list)
    return items_df


##job listing
def label_job_title(data, patterns, cate_name):
    """
    Input: data from Thinknum API
    Output: data unique listings with job title labelling
    """
    data_unique_listing = data.groupby(["POST_YMD" ,"Ticker Symbol", "Entity Name", "Listing ID", "URL", "Title"])["YMD"].max().reset_index()
    data_unique_listing.loc[data_unique_listing['Title'].str.contains(patterns), 'Job_Category'] = cate_name
    data_unique_listing["Job_Category"]= data_unique_listing["Job_Category"].fillna("other")
    return data_unique_listing
#weely/monthly/daily unique listing
def get_datelist(row, freq_value):
    """
    get datelist from start_date to end_date on a freq_value baisis
    """
    start_date = row["POST_YMD"]
    end_date = row["YMD"]
    datelist= list(pd.date_range(start=start_date, end = end_date, freq = freq_value))
    return datelist

def get_intersection(row, calendar_date,job_category):
    """
    update one row if date range overlaped
    """
    Job_Category = row["Job_Category"]
    unique_joblisting = len(set(row['date_range']).intersection(set([calendar_date])))
    row[Job_Category] = unique_joblisting
    return row

def get_unique_job_counts(row, data_df, job_category):
    calendar_date = row["date"]
    x = data_df.apply(lambda row:get_intersection(row, calendar_date, job_category), axis = 1)
    for job in job_category:
        row[job] = x[job].sum()
    return row

def get_active_data_summary(data_unique_listing, min_date, max_date, freq_value = "W"):
    """
    input: 
        unique job listing table with each job min listing date and max listing date;
        min_date: summary start date
        max_date: summary end date
        freq_value: "W", "M", "D"
    
    """
    data_summary = pd.DataFrame(pd.date_range(start=min_date, end = max_date, freq = freq_value, name = "date"))

    data_unique_listing["date_range"] = data_unique_listing[["POST_YMD","YMD"]].apply(lambda row: get_datelist(row, freq_value), axis = 1)
    job_category = data_unique_listing["Job_Category"].unique().tolist()
    
    start_time = time.time()
    d = dict.fromkeys(job_category, 0)
    data_summary = data_summary.assign(**d)
    data_unique_listing = data_unique_listing.assign(**d)
    data_summary = data_summary.apply(lambda row: get_unique_job_counts(row, data_unique_listing, job_category), axis = 1)
    print("--- %s seconds ---" % (time.time() - start_time))
    return data_summary

#job created (cumulative)
def __to_week_end(dtt):
    '''
    if using sunday as weekend
    '''    
    dt = pd.to_datetime(dtt)
    try:
        wd = dt.weekday()
        
    except:
        print("Error in parsing date {0}".format(dtt))
    
    return dt+datetime.timedelta(6-wd)

def add_week_month(data, date_column = "date"):
    #
    data['week'] = [__to_week_end(ll) for ll in data[date_column]]
    data['month'] = pd.to_datetime(data[date_column]).dt.strftime("%Y-%m")
    return data
def get_job_created_summary(data_unique_listing):
    """
    input : unique job listing table with each job min listing date and max listing date;
    output: job creation summary table with absolute and cumulative
    """
    data_created = data_unique_listing.groupby("POST_YMD").agg({"Listing ID": "nunique"}).reset_index().rename(columns = {"POST_YMD":"date","Listing ID": "Job_Created"})
    data_created['Cum_Sum_Job_Created'] = data_created.Job_Created.cumsum()
    data_created = add_week_month(data_created)
    return data_created