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


form_url = lambda c,q: 'http://'+SOLR_ENDPOINT+'/solr/'+c+'/select?q='+q+'&wt=json&indent=true&rows='+ROWS_COUNT

class Search_Query(Resource):
	"""
	class Search_Query: has methods to get search query from front end, return results
	"""
	def process_results(self, results):
		'''
		As of now, getting results obtained from Solr, writing them to a file, and performing tweet_analysis on it.
		Return results.
		TODO: Other functionalities
		'''

		print('--------------------------------------------------------------')		
		
		with open(JSON_FILENAME, "w") as write_file:
			json.dump(results, write_file)

		anal = tweet_analysis.tweet_anal(JSON_FILENAME)
		countries = anal.get_country_distro()
		print('Country distribution of tweets:',countries)

		print('--------------------------------------------------------------')


	def get_from_solr(self,core,query):
		'''
		Method to get data from given Solr core, call method process_results() on it, get country distribution
		TODO: Other functionalities
		'''

		core = 'IRF18P1'
		# core = 'p3'+core+'core'
		core_url = form_url(core, query)
		solr_results = json.loads(urllib.request.urlopen(core_url).read())
		countries = self.process_results(solr_results)
		'''
		TODO: Return countries distribution
		'''
		return solr_results

	def get(self, query):
		'''
		Endpoint for GET requests
		input: search query
		ret1,2,3 get results from the 3 different cores - vsm, bm25 and dfr
		TODO: Process these results to return top 20 common tweets from the 3 sets of results.
		'''

		ret1 = self.get_from_solr('vsm',query)
		# ret2 = self.get_from_solr('bm25',query)
		# ret3 = self.get_from_solr('dfr',query)
		if ret1:
			return ret1, 200
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