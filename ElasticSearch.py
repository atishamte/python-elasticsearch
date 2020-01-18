import requests
import json

class ElasticSearch:
	__index = None
	__server = None

	def __init__(self, server: str = None):
		if server is None or server == '':
			print("Server is not provided")
			exit()

		self.__server = server

	def set_index(self, index: str = None):
		if index is None or index == '':
			print("Index is not provided")
			exit()

		self.__index = index

		return self

	def create_index(self, index: str, mapping: dict = False):
		self.set_index(index)

		if not mapping:
			return self.__call(None, 'PUT')

		return self.__call(None, 'PUT', mapping)

	def delete_index(self):
		return self.__call(None, 'DELETE')

	def check_index(self):
		return self.__call()

	def status(self):
		return self.__call('_stats')

	def count_all(self):
		return self.__call('_count', 'GET', {"query": {"match_all": {}}})

	def add(self, identity: str, data: dict):
		return self.__call('_create/' + identity, 'PUT', data)

	def update(self, identity: str, data: dict):
		return self.__call('_update/' + identity, 'POST', data)

	def delete(self, identity: str):
		return self.__call('_doc/' + identity, 'DELETE')

	def get(self, identity: str):
		return self.__call('_doc/' + identity)

	def map(self, data: dict):
		query_param = {'properties': data}
		return self.__call('_mapping', 'PUT', query_param)

	def query(self, query: dict, size: int = 10):
		query_param = {
			'query': query,
			'size' : size
		}

		return self.__call('_search', 'POST', query_param)

	def more_like_this(self, fields: dict, data: dict, size: int = 10):
		fields_data = {k: v for k, v in fields.items() if v}
		if len(fields_data) == 0:
			print("Empty fields not allowed")
			exit()

		exe_data = {k: v for k, v in data.items() if v}
		if len(exe_data) == 0:
			print("Empty data not allowed")
			exit()

		filter = {
			'query': {
				'more_like_this': {
					'fields'       : fields_data,
					'like'         : exe_data,
					'min_term_freq': 1,
					'min_doc_freq' : 1,
				}
			},
			'size' : size
		}

		return self.__call('_search', 'POST', filter)

	def __call(self, path: str = None, method: str = 'GET', data: dict = None):

		url = self.__server + '/' + self.__index

		if path is not None:
			url += '/' + path

		method = method.upper()

		headers = {
			'Accept'      : 'application/json',
			'Content-Type': 'application/json'
		}
		response = None
		if method == 'GET':
			response_obj = requests.get(url, headers = headers)
			response = response_obj.text
		elif method == 'POST':
			post_data = data
			if data is not None:
				post_data = json.dumps(data)

			response_obj = requests.post(url, headers = headers, data = post_data)
			response = response_obj.text
		elif method == 'PUT':
			put_data = data
			if data is not None:
				put_data = json.dumps(data)

			response_obj = requests.put(url, headers = headers, data = put_data)
			response = response_obj.text
		elif method == 'DELETE':
			response_obj = requests.delete(url, headers = headers)
			response = response_obj.text
		else:
			print("Invalid method passed")
			exit()

		return json.loads(response)
