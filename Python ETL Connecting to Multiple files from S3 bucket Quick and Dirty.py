#!/usr/bin/env python
# coding: utf-8

# In[1]:


"""This script imports necessary libraries for data processing and AWS S3 interaction.

- pandas (pd): Used for data manipulation and analysis.
- numpy (np): Used for advanced numerical operations and arrays.
- boto3: Provides an interface to interact with Amazon Web Services (AWS), including Amazon S3.
- StringIO: A module for working with in-memory string buffers, often used for reading data from or writing data to strings.
"""
import boto3
import pandas as pd
import numpy as np
from io import StringIO, BytesIO
from datetime import datetime, timedelta


# In[2]:


s3 = boto3.resource('s3')

# Connecting to source S3 bucket
bucket = s3.Bucket('xetra-1234')


# In[3]:


# Setting an argument date from which we need data into our dataframe "2022-12-25" in our case
arg_date = '2022-12-25'

# To get the data of previous day's closing price we will need to fetch data from 2022-12-24
arg_date_datetime = datetime.strptime(arg_date, '%Y-%m-%d').date() - timedelta(days =1)
arg_date_datetime


# In[4]:


# Getting a list of all the objects/files inside S3 bucket
objects = [obj for obj in bucket.objects.all() if datetime.strptime(obj.key.split('/')[0], '%Y-%m-%d').date() >= arg_date_datetime]


# In[5]:


# We can see that list objects now contain details of all the files 
objects


# * we can see that "objects" now contains the list of all the files available inside the S3 Bucket named "xetra-1234" from date "2022-01-03" onwards

# In[6]:


# Lets get a list of column names of the data
csv_obj_init = bucket.Object(key = objects[0].key).get().get('Body').read().decode('utf-8')
data_init = StringIO(csv_obj_init)
df_init = pd.read_csv(data_init, delimiter = ',')


# In[7]:


# chechking the culumn names in dataframe
df_init.columns


# In[8]:


import warnings

# Filter out a specific warning
warnings.filterwarnings("ignore", category=FutureWarning)

# Creating an empty pandas dataframe and fixing the column names as per our dataset
df_merged = pd.DataFrame(columns=df_init.columns)

# Merging the data present in all the csv files into a single merged Dataset called df_merged using a for loop
for obj in objects:
    csv_obj = bucket.Object(key=obj.key).get().get('Body').read().decode('utf-8')
    data = StringIO(csv_obj)
    df = pd.read_csv(data, delimiter=',')
    df_merged = pd.concat([df_merged, df], ignore_index=True)


# In[9]:


df_merged


# In[10]:


# There are many columns which we do not need for further processing lets get rid of unwanted data
columns_to_included = ['ISIN', 'Date', 'Time', 'StartPrice', 'MaxPrice', 'MinPrice',
       'EndPrice', 'TradedVolume']


# In[11]:


# Filtering our merged dataframe to only those columns which we need for further process
df_merged = df_merged.loc[:,columns_to_included]
df_merged


# * df_merged now containes only those columns which we need for further analysis

# In[12]:


# Lets check our dataset for any Null Values
df_merged.isna().sum()


# ## Data Transformation

# In[13]:


# Lets check the dataframe to see whether we have any opening_price for the day
df_merged[(df_merged['ISIN'] == 'AT0000A0E9W5') & (df_merged['Date']== '2022-12-25')].sort_values(['Time'])


# * We can see that there is no column which provides us details about the opening price for a particular date
# * Now in the next steps we will create a few columns like opening_price, closing_price etc, which will be a part of our data transformation process.

# ### Get Opening price per ISIN per day

# In[14]:


# Creating a new column opening_price, which will be equal to the first reported StartPrice per ISIN per Day
df_merged['opening_price'] = df_merged.sort_values(['Time']).groupby(['ISIN', 'Date'])['StartPrice'].transform('first')
df_merged[(df_merged['ISIN'] == 'AT0000A0E9W5')]


# * Now we have a column which shows the opening price per ISIN per day.
# * We will now create some more columns in the same fashion which are needed for further analysis

# ### Get Closing price per ISIN per day

# In[15]:


# Creating a new column named closing_price
df_merged['closing_price'] = df_merged.sort_values(['Time']).groupby(['ISIN', 'Date'])['EndPrice'].transform('last')
df_merged[(df_merged['ISIN'] == 'AT0000A0E9W5')]


# ### Aggregations

# In[16]:


df_merged = df_merged.groupby(['ISIN', 'Date'], as_index=False).agg(opening_price_eur=('opening_price', 'min'), closing_price_eur = ('closing_price', 'min'), mimimum_price_eur = ('MinPrice','min'), maximum_price_eur = ('MaxPrice','max'), daily_traded_volume =('TradedVolume','sum'))


# In[17]:


df_merged


# * After the above transformations we have:
#     1. Opening Price Column for the day
#     2. Closing price column for the day
#     3. maximum price column for the day
#     4. minimum price column for the day
#     5. all the above columns show amount in Euros
#     6. Daily traded volumn for a particular stock for a particular day 

# ### Percent change previous closing

# In[18]:


# creating a new column previous_closiong_price and assigning the value of closing_price_eur column of previous day
df_merged['previous_closing_price'] = df_merged.sort_values(['Date']).groupby(['ISIN'])['closing_price_eur'].shift(1)

# Calculating precent change from previous day's closing price
df_merged['change_in_closing_in_%'] = round((df_merged['closing_price_eur'] - df_merged['previous_closing_price'])/df_merged['previous_closing_price']*100,2)

# Dropping column previous_closing_price as it is no longer needed
df_merged.drop(columns = ['previous_closing_price'], inplace = True)

# Lets round every number to upto two decimals
df_merged = df_merged.round(decimals = 2)
df_merged


# ### Write the combined and aggregated dataset to a target S3 Bucket

# In[19]:


# Describing the name of the key for output parquet file
key = 'xetra_daily_report' + datetime.today().strftime("%Y%m%d_%H%M%S")+'.parquet'


# In[21]:


# Creating an in-memory buffer using BytesIO
out_buffer = BytesIO()

# Writing the DataFrame (df_merged) to the in-memory buffer in Parquet format
# with index=False (no index column in the Parquet file)
df_merged.to_parquet(out_buffer, index=False)

# Creating an S3 bucket object by specifying the bucket name
bucket_target = s3.Bucket('xetra-1234-etl-target-bucket')

# Uploading the Parquet data (from the in-memory buffer) to the S3 bucket
# by getting the buffer's content with getvalue() and specifying the key
bucket_target.put_object(Body=out_buffer.getvalue(), Key=key)


# ### Reading the uploaded file to check whether the file content is correct

# In[23]:


# reading the name of parquet file from S3 bucket
for obj in bucket_target.objects.all():
    print (obj.key)


# In[25]:


# Getting the S3 object ('prq_obj') from the specified S3 bucket ('bucket_target')
# The object key is set to 'xetra_daily_report20231022_204256.parquet'
# The object is retrieved from S3 and its body is read as bytes
prq_obj = bucket_target.Object(key='xetra_daily_report20231022_204256.parquet').get().get('Body').read()

# Create a BytesIO object ('data') and load the Parquet data ('prq_obj') into it
data = BytesIO(prq_obj)

# Read the Parquet data from the BytesIO object into a pandas DataFrame ('df_report')
df_report = pd.read_parquet(data)


# In[ ]:




