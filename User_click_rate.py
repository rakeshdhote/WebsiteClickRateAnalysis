# -*- coding: utf-8 -*-
import pandas as pd
import pandasql
import numpy as np
import datetime
from dateutil.parser import parse
from zipfile import ZipFile

datafile = "ClickRate"

with ZipFile('{0}.zip'.format(datafile), 'r') as myzip:
    myzip.extractall()

#%%
# n-siginifanct digits
def nsf(num, n=1):
    """n-Significant Figures"""
    numstr = ("{0:.%ie}" % (n-1)).format(num)
    return float(numstr)

#%%
# rolling count
    # http://stackoverflow.com/questions/25119524/pandas-conditional-rolling-count
def rolling_count(val):
    if val == rolling_count.previous:
        rolling_count.count +=1
    else:
        rolling_count.previous = val
        rolling_count.count = 1
    return rolling_count.count
rolling_count.count = 0 #static variable
rolling_count.previous = None #static variable

#%%
df = pd.read_csv('hits.csv') #read dataframe

#%%
# 1 Average number of seconds between first and last visit (among those who visited more than once)*
df.time = df.time.apply(lambda d: datetime.datetime.strptime(d, "%Y-%m-%d %H:%M:%S")) # convert to date time stamp
pd.to_datetime(df.time)

grpu = df.groupby('user')
timediff_seconds = ( grpu['time'].agg(np.max) - grpu['time'].agg(np.min) ) / np.timedelta64(1, 's') # timediff_seconds = time diffence between first and last visit

y = timediff_seconds[timediff_seconds>0] # users who visited more than once
avg_sec_between_fl = np.mean(y) #avg_sec_between_fl = avg. seconds between first and last

print '1. Average number of seconds between first and last visit (among those who visited more than once) = ',  nsf(avg_sec_between_fl,10)

#%%
# 3 Average number of visits per user*
vpu = df.groupby(['user'],sort=True).count() #vpu = Visits_Per_User
avpu = np.mean(vpu['category']) # avpu = avg_Visits_Per_User

print '3. avg_Visits_Per_User = ', nsf(avpu,10)

#%%
#4 Average number of visits per user given they visited more than once*
vpum = vpu[vpu.category > 1] # vpum = visits per user given they visited more than once
avpum = np.mean(vpum['category']) # avpum = avg visits per user given they visited more than once

print '4. avg_Visits_Per_User (given they visited more than once) = ', nsf(avpum,10)

#%%
# 5 Average number of categories visited per user*
cvpu = df.groupby('user').category.nunique() # cvpu = categories visited per user
acvpu = np.mean(cvpu) # acvpu = average categories visited per user

print '5. avg_Catogery_Visits_Per_User = ', nsf(acvpu,10)

#%%
# 6 Average number of categories visited per user given they visitied more than once*
df_new = df.sort(['user'], ascending=1) #sort the dataframe by user
df_new = df_new.reset_index(drop=True) # reset index
df_new['count'] = df_new['category'].apply(rolling_count)

grouped = df_new.groupby('user') # group by user
catmax = grouped['count'].agg([np.max]) # catmax = find catogery maximum
acvpumoreo = np.mean(catmax[catmax>1])

print '6 Average number of categories visited per user given they visitied more than once = ', round(acvpumoreo,10)

#%%
# 7. Average number of categories visited per user given they visitied more than one category*
cvpumt1c = cvpu[cvpu > 1] # cvpumt1c = categories visited per user given they visitied more than one category
acvpumt1c = np.mean(cvpumt1c)

print '7. avg_Catogery_Visits_Per_User (more then one category) = ', nsf(acvpumt1c,10)

#%%
# 8 Probability of immediately visiting a page of the same category (given that the user visits again) evenly averaged over all categories.*

df_new = df.sort(['user'], ascending=1) #sort the dataframe by user
df_new = df_new.reset_index(drop=True) # reset index
df_new['block'] = (df_new['category'] != df_new['category'].shift(1)).astype(int).cumsum() #find cumulative sum for differnce in each category
df_new['blockn'] = df_new['block'].apply(rolling_count)
grpu = df_new.groupby('user') # group by user
avg_prob_ivpsc = np.mean(grpu['blockn'].agg(np.max) / grpu['blockn'].count()) # taking avg of (max visits in a group of a user/visits in groups by a user)

print '8. Probability of immediately visiting a page of the same category (given that the user visits again) evenly averaged over all categories.* = ', nsf(avg_prob_ivpsc,10)
#%%

