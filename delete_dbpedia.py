#!/usr/bin/env python
from elasticsearch import Elasticsearch, helpers
import json

es = Elasticsearch()

query_body = {
    "query": {
        "term": { "application" : "dbpedia" }
    }
}

scaniter = helpers.scan(client=es, query=query_body, scroll="1m", index='cendari', doc_type='entity')

def del_iterator():
    for rec in scaniter:
        yield { '_op_type': 'delete',
                '_index': 'cendari',
                '_type': 'entity',
                '_id': rec['_id'] }


with open('delete.out', 'wb') as out:
    ret = helpers.bulk(es, del_iterator())
    out.write(json.dumps(ret))

print 'Done.'
