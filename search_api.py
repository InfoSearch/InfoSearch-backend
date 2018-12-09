from flask import Flask, jsonify, request
from flask_cors import CORS, cross_origin
from flask_restful import Api, Resource, reqparse
import urllib.request, json
import pandas as pd
import tweet_analysis

app = Flask(__name__)
CORS(app)
api = Api(app)

# declaring constants
JSON_FILENAME = "data_file.json"
SOLR_HOSTNAME = 'ec2-18-224-141-235.us-east-2.compute.amazonaws.com'
SOLR_PORT = '8983'
SOLR_ENDPOINT = SOLR_HOSTNAME+':'+SOLR_PORT
CORE_NAME = 'project_final'


form_url = lambda c,q,rows: 'http://'+SOLR_ENDPOINT+'/solr/'+c+'/select?q='+urllib.request.quote(q.replace(' ','+'), safe='')+'&wt=json&indent=true&rows='+rows

class Search_Query(Resource):
	"""
	class Search_Query: has methods to get search query from front end, return results
	"""
	def process_results(self, results, countries_results):
		'''
		Getting results obtained from Solr, writing them to a file, and performing tweet_analysis on it. Return processed results.
		'''
		with open(JSON_FILENAME, "w") as write_file:
			json.dump(results, write_file)

		anal = tweet_analysis.tweet_anal(JSON_FILENAME)
		countries = anal.get_country_distro(countries_results)
		languages = anal.get_lang_distro()
		sentiments = anal.sentiment_analysis()
		tweets = anal.strip_tweets()

		res = {}
		res['result_count'] = results["response"]["numFound"]
		res['countries'] = countries
		res['languages'] = languages
		res['sentiments'] = sentiments
		res['tweets'] = tweets
		return res

	def get_from_solr(self,core,query):
		'''
		Method to get data from given Solr core, call method process_results() on it, get country distribution
		'''
		rows = '100'
		core_url = form_url(core, query, rows)
		rows = '100000'
		countries_url = form_url(core, query, rows)+'&fl=city'
		solr_results = json.loads(urllib.request.urlopen(core_url).read()).decode('utf-8'))
		if solr_results['response']['numFound'] == 0:
			return ''
		countries_results = json.loads(urllib.request.urlopen(countries_url).read()).decode('utf-8'))
		processed_results = self.process_results(solr_results,countries_results)
		return processed_results

	def get(self, query):
		'''
		Endpoint for GET requests
		input: search query
		Calls get_from_solr() which fetches results from Solr and calls process_results() to process and return results
		Return these results to the frontend API call
		'''
		ret = self.get_from_solr(CORE_NAME,query)
		if ret:
			return ret, 200
		else:
			return '', 404

api.add_resource(Search_Query, "/query/<string:query>")
app.run(host='0.0.0.0',debug=True)
