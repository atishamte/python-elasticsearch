# Python - ElasticSearch Library
**Technology Supports**
- Python 2.7+ / 3.7+
- ElasticSearch 7.5+
- requests package must be installed

A library to do the simple object operations with ElasticSearch

## Methods

```
from ElasticSearch import ElasticSearch

// If Elastic X-Pack authentication enabled
server = 'http://elastic:admin123@localhost:9200'

// If Elastic X-Pack authentication disabled
server = 'http://localhost:9200'

// Name of the index
index = 'dummydata'

// New elasticsearch object 
e = ElasticSearch(server);
--------------------------------------------------------
// Create Index
e.create_index(index)
--------------------------------------------------------
// Setting a index to object
e.set_index(index)
--------------------------------------------------------
// Check Index

// Without method chaining
e.set_index(index)
e.check_index()

// With method chaning
e.set_index(index).check_index()
--------------------------------------------------------
// Check status of Index
e.status()
--------------------------------------------------------
// Add document in index
e.add('person', {"name": "Robert"})
--------------------------------------------------------
// Get document from index
e.get("person")
--------------------------------------------------------
// Update document in index
e.update('person', {"doc": {"name": "Robert Downy Jr"}})
--------------------------------------------------------
// Query document from index
e.query({"match": {"name": "Robert Downy Jr"}})
--------------------------------------------------------
// Count all documents in index
e.count_all()
--------------------------------------------------------
// Delete document from index
e.delete("person")
--------------------------------------------------------
// Delete Index
e.delete_index()
```
