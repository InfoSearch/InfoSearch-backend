import pandas as pd
import numpy as np
from collections import Counter
from pandas.io.json import json_normalize
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer


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
        self.df = self.df.drop(self.df.columns[0],axis=1)
        self.analyser = SentimentIntensityAnalyzer()
    
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
        countries_res = {}
        countries_res["countries"] = countries
        return countries_res
    
    def get_lang_distro(self):
        langs = dict(Counter(self.df['language_s']))
        langs_res = {}
        langs_res["languages"] = langs
        return langs_res
    
    def strip_tweets(self):
        drop_range=np.r_[1:57,59:66,68:75,85:129]
        self.df = self.df.drop(self.df.columns[drop_range], axis=1)
        tweets = self.df.to_json(orient='index')
        tweets_res = {}
        tweets_res["tweets"] = tweets
        return tweets_res
    
    def sentiment_analysis(self):
        tweets = [d for d in self.df['text_en'] if d == d]
        scores = []
        for t in tweets:
            scores.append(self.sentiment_analyzer_scores(t))
        scores_res = {}
        scores_res["sentiments"] = scores
        return scores_res
        
    def sentiment_analyzer_scores(self,sentence):
        score = self.analyser.polarity_scores(sentence)
        return score