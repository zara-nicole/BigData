# -*- coding: utf-8 -*-
"""
Created on Mon Jul 02 09:42:17 2018

@author: zsaldanh
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os
import seaborn as sns
import glob
import re
from datetime import datetime

data = pd.read_csv('food_fb.csv')
data['As Of Date'] = pd.to_datetime(data['As Of Date'], format = "%m/%d/%Y %H:%M")


data=data.groupby(["Username"]).apply(lambda x: x.sort_values(["As Of Date"]))
