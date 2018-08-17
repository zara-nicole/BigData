###Thinknum API pull
import thinknum_pull
import json

#test 0 - get data fields (max: 1000 records)    
dataset_name = 'community_members'
form_data = {
    'request': json.dumps(
        {
            'tickers': [],
        }
    ),
    'start': 1,
    'limit': 1000
}

test = thinknum_pull._get_data_fields(dataset_name, form_data)

#test 1 - get data with single loop (max: 1000 records)    
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

test = thinknum_pull._get_data_single_loop(dataset_name, form_data)


#test 2 - get data with multi-loop (max: max limitations)
dataset_name = 'job_listings'
form_data = {
    'request': json.dumps(
        {
            'tickers': ['nasdaq:goog'],
        }
    ),
    'start': 1,
    'limit': 1000,
}
test = thinknum_pull._get_data_multi_loop(dataset_name, form_data)    
