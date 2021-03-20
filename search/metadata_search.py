import requests
import json
from time import sleep
from elasticsearch import Elasticsearch

from itertools import islice
             
client = Elasticsearch()
es_index = 'danae-eodp'

class MetadataSearcher:
    
    def __init__(self, M=1000):
        self.M = M
        
    def search(self, kwds="", title="", description=""):
        query = {"_source": ["_id", "metadata.id", "metadata.title"], "track_scores":"true",
                 "query": {"bool": {"should": [{"match": {"metadata.keywords": {"query": kwds, "boost": 4}}},
                                               {"match": {"metadata.title": {"query": title, "boost": 0}}},
                                               {"match": {"metadata.description": {"query": description, "boost": 1}}}
                               ],
                    "minimum_should_match" : 1 }}, "size": self.M}
        
 
        response = client.search(index=es_index, body=query)
        
        out = []
        max_score = response['hits']['max_score']
        for hit in response['hits']['hits']:
            title = hit['_source']['metadata']['title'] if 'metadata' in hit['_source'] and 'title' in hit['_source']['metadata'] else ""
            out.append((hit['_id'], hit['_score']/max_score, hit['_source']['metadata']['id'], title))
        return out
        
        
