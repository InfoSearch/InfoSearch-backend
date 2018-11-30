
# coding: utf-8

# In[182]:


import pandas as pd
import numpy as np
from collections import Counter
from pandas.io.json import json_normalize

class tweet_anal:
    # city to country mapping -  to be extended after pooling all tweets and tracking all unique cities present
    cities_countries = {'nyc':'usa','berlin':'germany',
                        'bangkok':'thailand','paris':'france',
                        'mexico city':'mexico','delhi':'india'}

    def __init__(self,filename):
        '''
        Initialize: Read json into pandas DataFrame, extract tweets from Solr's JSON response, normalize
        '''
        self.df = pd.read_json(filename)
        self.df = json_normalize(self.df['response']['docs'])
        # self.df = self.df.drop(self.df.columns[0],axis=1)
    
    def get_country_distro(self):
        '''
        Extracting country distribution from tweets for displaying heatmap
        ''' 
        counts =  dict(Counter([i[0] for i in self.df['city'].replace(np.nan, '', regex=True) if i]))
        countries = {}
        for c in counts:
            if c in self.cities_countries:
                countries[self.cities_countries[c]] = counts[c]
            else:
                # making usa the default country for all uncaught cities
                countries['usa']+=counts[c]
        return countries