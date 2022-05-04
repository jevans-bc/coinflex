#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Apr 15 04:12:09 2022

@author: josevans
"""

import os
import glob
import pandas as pd
# import math
os.chdir("/home/ec2-user/csvFiles")

extension = 'csv'
all_filenames = [i for i in glob.glob('*.{}'.format(extension)) if 'orders' in i]


#combine all files in the list
combined_csv = pd.concat([pd.read_csv(f) for f in all_filenames ])
#export to csv
combined_csv.to_csv( "combined_os.csv", index=False, encoding='utf-8-sig')
# ammAccounts = combined_csv['Unnamed: 0'].unique()

# count = 0

# for i in range(math.ceil(len((ammAccounts))/10+1)):
    
#     accs = ammAccounts[count:count+10]
    
#     string = ''
    
#     for acc in accs:
        
#         string += '{}'.format(acc)+','
    
#     r = private_rest('/v3/account?subAcc="'+string[:-1]+'"','GET')
    
#     return r.json()['data']
        
#     count+=10
