# -*- coding: utf-8 -*-
"""
Created on Thu Jun 07 11:59:44 2018

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