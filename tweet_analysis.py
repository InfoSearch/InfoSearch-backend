import pandas as pd
import numpy as np
import json
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
        self.df = self.df.replace(np.nan, '', regex=True)
        self.df['tweet_id'] = [str(ti[0]) for ti in self.df['tweet_id']]        
    
    def get_country_distro(self,countries_results):
        '''
        Extracting country distribution from tweets for displaying heatmap
        ''' 
        json_countries = json.loads(json.dumps(countries_results))
        countries_df = json_normalize(json_countries['response']['docs'])
        counts = dict(Counter([i[0] for i in countries_df['city'].replace(np.nan, '', regex=True) if i]))

        countries = {}
        for c in counts:
            if c in self.cities_countries:
                countries[self.cities_countries[c]] = counts[c]
            else:
                # making usa the default country for all uncaught cities
                countries['usa']+=counts[c]

        return countries
    
    def get_lang_distro(self):
        li = []
        for d in self.df['tweet_lang']:
            li.append(d[0])
        self.df['language_s'] = li
        langs = dict(Counter(self.df['language_s']))
        return langs
    
    def strip_tweets(self):
        tweets = self.df.to_dict(orient='records')
        return tweets
    
    def sentiment_analysis(self):
        if 'text_en' not in self.df.columns:
            return []
        tweets = [d for d in self.df['text_en'] if d == d]
        scores = []
        for t in tweets:
            scores.append(self.sentiment_analyzer_scores(t))
        overall_sentiment = {}
        overall_sentiment['pos'], overall_sentiment['neu'], overall_sentiment['neg'] =0, 0, 0
        for s in scores:
            pos, neu, neg = s['pos'], s['neu'], s['neg']
            if(pos > neu and pos > neg):
                overall_sentiment['pos'] += 1
            elif (neg > pos and neg > neu):
                overall_sentiment['neg'] += 1
            elif (neu > neg and neu > pos):
                overall_sentiment['neu'] += 1

        sentiment_results  = {}
        sentiment_results['overall_sentiment'] = overall_sentiment
        sentiment_results['scores'] = scores

        return sentiment_results
        
    def sentiment_analyzer_scores(self,sentence):
        score = self.analyser.polarity_scores(sentence)
        return score