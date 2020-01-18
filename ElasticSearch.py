import requests
import json


class ElasticSearch:
	'''
	This is a ElasticSearch Class
	'''
	__index = None
	__server = None

	def __init__(self, server: str = None):
		"""__init__ method
        Parameters
        ----------
        server : str
            The name of the server

        Raises
        ------
        ValueError
            If no server value passed as a parameter.
        """
		if server is None or server == '':
			raise ValueError("Server is not provided")

		self.__server = server

	def __repr__(self):
		return "This is the Python - ElasticSearch Library to do the simple object operations with ElasticSearch"

	def set_index(self, index: str = None):
		"""Set the index

        Parameters
        ----------
        index : str
            The name of the index

        Raises
        ------
        ValueError
            If no index value passed as a parameter.
        """
		if index is None or index == '':
			raise ValueError("Index is not provided")

		self.__index = index

		return self

	def create_index(self, index: str, mapping: dict = False):
		"""To create a index with mapping or not

        Parameters
        ----------
        index : str
            The name of the index

        mapping : dict, optional
            mapping for the properties

        Raises
        ------
        ValueError
            If no index value passed as a parameter.
        """
		self.set_index(index)

		if not mapping:
			return self.__call(None, 'PUT')

		return self.__call(None, 'PUT', mapping)

	def delete_index(self):
		"""Delete the index"""

		return self.__call(None, 'DELETE')

	def check_index(self):
		"""Check index is available"""
		return self.__call()

	def status(self):
		"""Get status"""
		return self.__call('_stats')

	def count_all(self):
		"""Get the index count"""
		return self.__call('_count', 'GET', {"query": {"match_all": {}}})

	def add(self, identity: str, data: dict):
		"""Add the document

		Parameters
		----------
		identity : str
			The name of the document

		data : dict
			Data to be inserted
		"""
		return self.__call('_create/' + identity, 'PUT', data)

	def update(self, identity: str, data: dict):
		"""Update the document

		Parameters
		----------
		identity : str
			The name of the document

		data : dict
			Data to be updated
		"""
		return self.__call('_update/' + identity, 'POST', data)

	def delete(self, identity: str):
		"""Delete the document

		Parameters
		----------
		identity : str
			The name of the document
		"""
		return self.__call('_doc/' + identity, 'DELETE')

	def get(self, identity: str):
		"""Fetch single document by id

		Parameters
		----------
		identity : str
			The name of the document
		"""
		return self.__call('_doc/' + identity)

	def map(self, data: dict):
		"""To set the mapping for the document

		Parameters
		----------
		data : dict
			Data to be mapped with document
		"""
		query_param = {'properties': data}
		return self.__call('_mapping', 'PUT', query_param)

	def query(self, query: dict, size: int = 10):
		"""Search by query

		Parameters
		----------
		query : dict
			Query to find the data

		size : int, optional
			Number of records to be fetch
		"""
		query_param = {
			'query': query,
			'size' : size
		}

		return self.__call('_search', 'POST', query_param)

	def more_like_this(self, fields: dict, data: dict, size: int = 10):
		"""To set the mapping for the document

		Parameters
		----------
		fields : dict
			Fields to be filtered

		data : dict
			Date to be find in documents

		size : int, optional
			Number of records to be fetch
		"""
		fields_data = {k: v for k, v in fields.items() if v}
		if len(fields_data) == 0:
			raise ValueError("Empty fields not allowed")

		exe_data = {k: v for k, v in data.items() if v}
		if len(exe_data) == 0:
			raise ValueError("Empty data not allowed")

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
			raise ValueError("Invalid method passed")

		return json.loads(response)
