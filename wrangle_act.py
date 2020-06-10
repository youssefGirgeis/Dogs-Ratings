#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import requests
import json
import tweepy


# In[2]:


consumer_key = 'VlFaFpY1w2oaWTN3lItGykJS4'
consumer_secret = 'It5cUQJ6CWBF13n6AUpHx0eHslR5r0NXIQLkebDKGMrfkuaSWv'
access_token = '2685226321-7KE4KEqMLCZeht5tcRxNfCJ8Iduo9K4feEsX5DR'
access_secret = 'QkPPBiJaLBtkh4zokD1qVtBiV5DBA2a21wZM6OkpWIWGW'

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)

api = tweepy.API(auth)


# ## 1- Data Gathering

# In[3]:


# create the first dataframe to store twitter-archive-enhanced.csv file
df_twitter_archive = pd.read_csv('twitter-archive-enhanced.csv')


# In[4]:


# downloading image_predictions.tsv file programmatically
url = 'https://d17h27t6h515a5.cloudfront.net/topher/2017/August/599fd2ad_image-predictions/image-predictions.tsv'
response = requests.get(url)

#writing the content of response to a file
with open(url.split('/')[-1], mode='wb') as file:
    file.write(response.content)
    
# create the second dataframe to store the image_predications.tsv file
df_dog_breeds = pd.read_csv('image-predictions.tsv', sep='\t')


# In[5]:


# store tweets data in tweet_json.txt file

fails_dict= {} # tweets we couldn't find
with open ('tweet_json.txt', 'w') as file:
    for tweet_id in df_twitter_archive.tweet_id:
        try:
            tweet = api.get_status(tweet_id, tweet_mode = 'extended')
            json.dump(tweet._json, file)
            file.write('\n')
        except tweepy.TweepError as e:
            fails_dict[tweet_id] = e
            pass


# In[6]:


# read the "tweet_json.txt" line be line and
# each line is a dictionary, so can access the data we need
# in each line and store in list. After we finish, we will
# convert the list of dictionary into a datafram

tweets_list = []
with open('tweet_json.txt', 'r') as json_file:
    for line in json_file:
        json_line = json.loads(line)
        tweets_list.append({
            'tweet_id': json_line['id'],
            'created_at': json_line['created_at'],
            'retweet_count': json_line['retweet_count'],
            'favorite_count': json_line['favorite_count']
        })


# In[7]:


# create the third dataframe df_tweets
df_tweets = pd.DataFrame(tweets_list, columns=['tweet_id', 'retweet_count', 'favorite_count', 'created_at'])


# In[8]:


df_twitter_archive


# In[9]:


df_dog_breeds


# In[10]:


df_tweets


# ## 2- Assess Data

# In[11]:


df_twitter_archive.info()


# - timestamp shouldn't of type oject
# - missing values in mutiple columns (in_reply_to_status_id, in_reply_to_user_id, retweeted_status_id, retweeted_status_user_id, retweeted, status_timestamp)
# - dog stages are in four columns

# In[12]:


df_dog_breeds.info()


# - tweet_id should of type object not int

# In[13]:


df_tweets.info()


# - tweet_id column should be string not int
# - created_at column should be date not string.

# In[14]:


df_twitter_archive


# In[15]:


df_twitter_archive[['tweet_id','text', 'rating_numerator','rating_denominator', 'name']]


# - at tweet_id = 666287406224695296, rating_numerator and rating_denominator are extrated wrong. they should be 9 and 10 instead of 1 and 2

# In[16]:


df_twitter_archive.rating_denominator.value_counts()


# - some values in the rating_denominator are greater than 10

# In[17]:


df_twitter_archive.name.value_counts()


#  - there are values in the name column extracted wrong. As you can see above, there are 745 values = "None", 55 = the "a" letter. 

# In[18]:


df_twitter_archive.rating_numerator.value_counts()


# - outliers in the rating_numerator column

# In[19]:


df_twitter_archive[['tweet_id','text', 'rating_numerator','rating_denominator']][df_twitter_archive.rating_numerator > 14]


# - decimal ratings aren't extracted properly (tweet_id = 832215909146226688)

# ### Data Quality Issues
# - tweet_id in the three dataframe is int.It shold be string
# - timestamp in the df_arhcive is string
# - missing values in mutiple columns (in_reply_to_status_id, in_reply_to_user_id, retweeted_status_id, retweeted_status_user_id, retweeted, status_timestamp)
# - some values in the rating_denominator are greater than 10
# - retweets in the df_archive table
# - there are values in the name column extracted wrong from the tweet's text
# - outliers in the rating_numerator column
# - at tweet_id = 666287406224695296, rating_numerator and rating_denominator are extrated wrong. they should be 9 and 10 instead of 1 and 2
# - decimal ratings aren't extracted properly (tweet_id = 786709082849828864)
# - ratings should be float ranging since we have decimal values
# 
# 
# 
# ### Data Tidiness Issues
# - dog stages are in four columns
# - tweets info separated in two tables (df_archive_tweets and df_tweets)
# - dog breed should be a column in the combined table

# ## 3- Data Cleaning

# In[20]:


# create a copy df_twitter_archiver. This copy will be the cleaned version 
df_twitter_archive_cleaned = df_twitter_archive.copy()


# In[21]:


df_tweets_clean = df_tweets.copy()
df_dog_breeds_clean = df_dog_breeds.copy()


# ### Define
# - remove the retweets data from the df_twitter_archive table

# ### Code

# In[22]:


# fitering out retweets
df_twitter_archive_cleaned = df_twitter_archive_cleaned[df_twitter_archive.retweeted_status_id.isna()]


# ### test

# In[23]:


df_twitter_archive[df_twitter_archive.name == 'Canela']


# In[24]:


df_twitter_archive_cleaned[df_twitter_archive_cleaned.name == 'Canela']


# #### As you can see above, the original dataframe has the original tweet and the retweet. The cleaned dataframe doesn't have the retweet

# In[25]:


# check if there are data in the retweeted_status_id column
df_twitter_archive_cleaned[~df_twitter_archive_cleaned.retweeted_status_id.isna()]


# In[26]:


# check if there are data in the retweeted_status_user_id column
df_twitter_archive_cleaned[~df_twitter_archive_cleaned.retweeted_status_user_id.isna()]


# In[27]:


# check if there are data in the rretweeted_status_timestamp column
df_twitter_archive_cleaned[~df_twitter_archive_cleaned.retweeted_status_timestamp.isna()]


# ### Missing data

# In[28]:


df_twitter_archive_cleaned.info()


# ### Define
# - Remove missing data
# - we can drop retweeted_status_id, retweeted_status_user_id, retweeted_status_timestamp since they don't have any data
# - we can also drop in_reply_to_status_id and in_reply_to_user_id since they a lot of missing data and we won't use them ain our analysis 

# ### Code

# In[29]:


# drop columns with missing values
df_twitter_archive_cleaned.drop(['retweeted_status_id', 'retweeted_status_user_id', 'retweeted_status_timestamp',
                                 'in_reply_to_status_id', 'in_reply_to_user_id'], axis=1, inplace = True)


# ### Test

# In[30]:


df_twitter_archive_cleaned.info()


# ### Define
# - Create one variable/column for the four dog stages

# ### Code

# In[31]:


# reaplce None with empty string in the four columns
df_twitter_archive_cleaned.doggo.replace('None','', inplace=True)
df_twitter_archive_cleaned.floofer.replace('None','', inplace=True)
df_twitter_archive_cleaned.pupper.replace('None','', inplace=True)
df_twitter_archive_cleaned.puppo.replace('None','', inplace=True)
df_twitter_archive_cleaned[['text', 'doggo', 'floofer', 'pupper', 'puppo']]


# In[32]:


# create a new column (dog_stage) for the dog stages
df_twitter_archive_cleaned['dog_stage'] = df_twitter_archive_cleaned.doggo + df_twitter_archive_cleaned.floofer + df_twitter_archive_cleaned.pupper + df_twitter_archive_cleaned.puppo


# In[33]:


df_twitter_archive_cleaned.dog_stage.value_counts()


# #### As you can see above there are some dogs with muliple stages.  we can sperate those instances with commas

# In[34]:


df_twitter_archive_cleaned.loc[df_twitter_archive_cleaned.dog_stage == 'doggopupper', 'dog_stage'] = 'doggo, pupper'
df_twitter_archive_cleaned.loc[df_twitter_archive_cleaned.dog_stage == 'doggofloofer', 'dog_stage'] = 'doggo, floofer'
df_twitter_archive_cleaned.loc[df_twitter_archive_cleaned.dog_stage == 'doggopuppo', 'dog_stage'] = 'doggo, puppo'


# In[35]:


df_twitter_archive_cleaned.dog_stage.value_counts()


# #### we can now convert all the empty strings to NaN

# In[36]:


df_twitter_archive_cleaned.dog_stage.replace('', np.nan, inplace=True)


# #### Now we have one column that represents the dog stages. we can delete the other four columns

# In[37]:


df_twitter_archive_cleaned.drop(['doggo', 'floofer', 'pupper', 'puppo'], axis=1, inplace=True)


# ### Test

# In[38]:


df_twitter_archive_cleaned.dog_stage.value_counts()


# In[39]:


df_twitter_archive_cleaned.info()


# ### Define 
# - Merge df_twitter_archive_cleaned and df_tweets_cleaned in a new table (df_all_tweets)
# - Before we merge the two tables let change the type of tweet_id in both tables to string/object
# - I decided to delete the "created_at" in the df_tweets table since it is a duplicate

# ### Code

# In[40]:


# change the type of tweet_id in both tables to string/object
df_twitter_archive_cleaned.tweet_id = df_twitter_archive_cleaned.tweet_id.astype(str)
df_tweets_clean.tweet_id = df_tweets_clean.tweet_id.astype(str)
df_tweets_clean .drop('created_at', axis=1, inplace=True)


# In[67]:


df_all_tweets = df_twitter_archive_cleaned.merge(df_tweets_clean, on='tweet_id', how='left')


# ### Test

# In[42]:


df_all_tweets.info()


# ### Define
# - change type of tweet_id in df_dog_breeds_clean table to string/object
# - create a new column "dog_breed" in df_dog_breeds table.
# - merge df_all_tweets and df_dog_breeds_clean

# In[43]:


# change type of tweet_id in df_dog_breeds_clean table to string/object
df_dog_breeds_clean.tweet_id = df_dog_breeds_clean.tweet_id.astype(str)


# In[44]:


# function below is responsible for populating the new column "dog_breed"
def get_dog_breed(df):
    if (df.p1_dog == True):
        return df.p1
    elif (df.p2_dog == True):
        return df.p2
    elif (df.p3_dog == True):
        return df.p3
    else:
        return np.nan


# In[45]:


# add new column to the dataframe
df_dog_breeds_clean['dog_breed'] = df_dog_breeds_clean.apply(get_dog_breed, axis=1)


# In[46]:


df_dog_breeds_clean


# In[47]:


df_all_tweets = df_all_tweets.merge(df_dog_breeds_clean[['tweet_id','dog_breed','jpg_url']], on='tweet_id', how='left')


# ### Test

# In[48]:


df_all_tweets.info()


# In[49]:


df_all_tweets


# ### Define
# 
# - Fix rating_numerator and rating_denominator values at row 2335

# ### Code

# In[70]:


# replace the value of rating_numerator = 1 with 9
df_all_tweets.loc[df_all_tweets['tweet_id']=='666287406224695296', 'rating_numerator'] = 9


# In[72]:


# replace the value of rating_denominator = 2 with 10
df_all_tweets.loc[df_all_tweets['tweet_id']=='666287406224695296', 'rating_denominator'] = 10


# ### Test

# In[73]:


df_all_tweets[df_all_tweets.tweet_id == '666287406224695296']


# ### Define
# - Set the "rating_denominator" column data to 10

# ### Code

# In[74]:


df_all_tweets.rating_denominator = 10


# ### Test

# In[75]:


df_all_tweets.rating_denominator.sample(5)


# ### Define
# - Replace wrong dog names with nan

# In[80]:


#correct names starts wwith capital letter
# some of the wrong names starts with a lower case letter

df_all_tweets.loc[df_all_tweets.name.str.islower(), 'name'] = np.nan


# In[82]:


pd.set_option('display.max_colwidth', -1)
df_all_tweets[['text','name']][df_all_tweets.name == 'None']


# In[83]:


# we should also replace names = "None" with nan
df_all_tweets.name.replace('None', np.nan, inplace = True)


# ### Test

# In[85]:


# test "None" values
df_all_tweets.name.value_counts()


# In[88]:


# test lower case values
df_all_tweets.name.str.islower().sum()


# ### Define
# - Extract the decimal ratings properly (tweet_id = 832215909146226688)

# In[101]:


df_all_tweets[df_all_tweets.tweet_id == '786709082849828864']


# In[ ]:





# ### Code

# In[107]:


df_all_tweets.rating_numerator = df_all_tweets.text.str.extract('(\d*\.?\d+)', expand=False).astype(float)


# ### Test

# In[108]:


df_all_tweets[df_all_tweets.tweet_id == '786709082849828864']


# In[ ]:





# In[113]:


df_all_tweets.rating_numerator.value_counts()


# In[ ]:




