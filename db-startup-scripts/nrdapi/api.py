from flask import Flask, jsonify, request
from flask_restful import Resource, Api, reqparse
import pandas as pd
from string import Template
import ast
import json
import os


app = Flask(__name__)

script_dir = os.path.dirname(__file__)
FILE_ROUTING_TEMPLATE = Template("expect-store/${dataset_name}/demo.json")
# FILE_ROUTING_TEMPLATE = Template("expects/${dataset_name}/demo.json")


@app.route('/', methods=['GET'])
def get_ln():
	return {"access_token":"a"}, 200

@app.route('/', methods=['POST'])
def post_ln():
	return {"access_token":"a"}, 200


@app.route('/api/Expectation/Suite/<dfirst>/<dname>/demo')
def esuite(dfirst,dname):
	script_dir = os.path.dirname(__file__)
	dataset_n = dfirst + '_' + dname
	expect_file = FILE_ROUTING_TEMPLATE.substitute(dataset_name=dataset_n)
	full_expect_loc = os.path.join(script_dir, expect_file)
	f = open(full_expect_loc,)
	expects = json.load(f)
	print()
	return jsonify({'expectations': expects['expectations'], 'status_code': 200}), 200


@app.errorhandler(404) 
def invalid_route(e): 
	return jsonify({'status_code' : 204, 'message' : 'Route not found'}), 204

if __name__ == '__main__':
	app.run(host='0.0.0.0', port=5000)