import requests
import json

from document import Document
from theExceptions import QueryBatchRetrievalError, QueryError, SimpleQueryError

class QueryResult(object) :
	"This class abstract and should not be instanciated"
	
	def __init__(self, request, rawResults) :
		"if rawResults = True, will always return the json representation of the results and not a Document object. queryPost contains a dictionnary representation of the initial POST payload sent to the database"

		self.rawResults = rawResults
		self.response = request.json()

		if request.code == 404 :
			self.batchNumber = 0
			self.result = []
		elif request.code = 201 :
			self.batchNumber = 1
			if self.response["hasMore"] :
				self.cursorUrl = "http://localhost:8529/_db/test_db/_api/cursor/%s" % (self.id)
			else :
				self.cursorUrl = None
			self._developed = set()
		else :
			raise self._raiseInitFailed(request)
	
	def _raiseInitFailed(self, request) :
		"must be implemented in child, this called if the __init__ fails"
		raise NotImplemented("Must be implemented in child")

	def _developDoc(self, i) :
		"must be implemented in child transform a dict into a Document object"
		raise NotImplemented("Must be implemented in child")

	def nextBatch(self) :
		"become the next batch. raises a StopIteration if there is None"
		self.batchNumber += 1
		if not self.response["hasMore"] :
			raise StopIteration("That was the last batch")

		r = requests.put(self.cursorUrl)
		self.response = r.json()
		if self.response['error'] :
			raise QueryBatchRetrievalError(self.response["errorMessage"], self.batchNumber, self.response)
		self._developed = set()

	def delete(self) :
		"kills the cursor"
		requests.delete(self.cursorUrl)

	def __getitem__(self, i) :
		"returns a ith result of the query."
		if not self.rawResults and (i not in self._developed) : 
			self._developDoc(i)
			self._developed.add(i)
		return self.result[i]

	def __len__(self) :
		return len(self.result)

	def __getattr__(self, k) :
		try :
			return self.response[k]
		except KeyError:
			raise  AttributeError("%s has not attribute %s" %(self.__class__.name, k))

class AQLQueryResult(QueryResult) :
	"AQL queries are attached to a database"
	def __init__(self, database, query, rawResults, batchSize, bindVars, options, count, fullCount) :
		payload = {'query' : query, 'batchSize' : batchSize, 'bindVars' : bindVars, 'options' : options, 'count' : count, 'fullCount' : fullCount}
		
		self.query = query
		self.database = database
		r = requests.post(databse.cursorsURL, data = json.dumps(payload))
		QueryResult.__init__(self, request, rawResults)

	def _raiseInitFailed(self, request) :
		data = request.json()
		raise AQLQueryError(data["errorMessage"], self.query, data)

	def _developDoc(self, i) :
		docJson = self.result[i]
		try :
			collection = self.database[docJson["_id"].split("/")[0]]
		except KeyError :
			raise CreationError("result %d is not a valid Document. Try setting rawResults to True" % i)

		self.result[i] = Document(collection, docJson)

class SimpleQueryResult(QueryResult) :
	"Simple queries are attached to a single collection"
	def __init__(self, collection, queryType, batchSize, rawResults, queryArgs) :

		self.collection = collection
		payload = {'collection' : collection.name, 'batchSize' : batchSize}
		payload.update(queryArgs)
		payload = json.dumps(payload)
		URL = "%s/simple/%s" % (self.database.URL, queryType)

		request = requests.put(URL, data = payload)

		QueryResult.__init__(self, request, rawResults)

	def _raiseInitFailed(self, request) :
		data = request.json()
		SimpleQueryError(data["errorMessage"], data)

	def _developDoc(self, i) :
		docJson = self.result[i]
		self.result[i] = Document(self.collection, docJson)