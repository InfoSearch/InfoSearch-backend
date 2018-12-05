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
SOLR_HOSTNAME = 'localhost'
SOLR_PORT = '8983'
ROWS_COUNT = '100'
SOLR_ENDPOINT = SOLR_HOSTNAME+':'+SOLR_PORT
CORE_NAME = 'IRF18P1'


form_url = lambda c,q: 'http://'+SOLR_ENDPOINT+'/solr/'+c+'/select?q='+q+'&wt=json&indent=true&rows='+ROWS_COUNT

class Search_Query(Resource):
	"""
	class Search_Query: has methods to get search query from front end, return results
	"""
	def process_results(self, results):
		'''
		Getting results obtained from Solr, writing them to a file, and performing tweet_analysis on it. Return processed results.
		'''
		num_res = {}
		num_res["result_count"] = results["response"]["numFound"]
		with open(JSON_FILENAME, "w") as write_file:
			json.dump(results, write_file)
		
		anal = tweet_analysis.tweet_anal(JSON_FILENAME)
		countries = anal.get_country_distro()
		languages = anal.get_lang_distro()
		sentiments = anal.sentiment_analysis()
		tweets = anal.strip_tweets()
		
		res = [num_res, countries,languages,sentiments,tweets]
		json_ret = json.dumps(res)
		print(json_ret)
		return json_ret

	def get_from_solr(self,core,query):
		'''
		Method to get data from given Solr core, call method process_results() on it, get country distribution
		'''
		core_url = form_url(core, query)
		solr_results = json.loads(urllib.request.urlopen(core_url).read())
		processed_results = self.process_results(solr_results)
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

	# def post(self, name):
	# 	parser = reqparse.RequestParser()
	# 	parser.add_argument("age")
	# 	args = parser.parse_args()

	# 	for user in users:
	# 		if name == user["name"]:
	# 			return "User with name {} already exists".format(name), 400
	# 	user = {"name":name,"age":args["age"]}
	# 	users.append(user)
	# 	return user, 201

api.add_resource(Search_Query, "/query/<string:query>")
app.run(debug=True)