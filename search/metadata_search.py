from elasticsearch import Elasticsearch
import re
from json import load

class MetadataSearcher:
    
    def __init__(self, M=1000):
        self.M = M
        self.last_query = ""
        self.matchings = {}
        
    def search(self, res, fields):
        nested = []
        if 'keywords' in fields and 'keywords' in res['_source']['metadata']:
            kwds = res['_source']['metadata']['keywords']
            nested += [{"match": {"metadata.keywords": {"query": kwds, "boost": fields['keywords']}}}]

        if 'title' in fields and 'title' in res['_source']['metadata']:
            title = res['_source']['metadata']['title']
            nested += [{"match": {"metadata.title": {"query": title, "boost": fields['title']}}}]
            
        if 'description' in fields and 'description' in res['_source']['metadata']:
            description = res['_source']['metadata']['description']          
            nested += [{"match": {"metadata.description": {"query": description, "boost": fields['description']}}}]
            
        if len(nested) == 0:
            return []
        
        query = {"_source": ["_id", "metadata.id", "metadata.title"], "track_scores":"true",
                 "query": {"bool": {"should": nested,
                                    "minimum_should_match" : 1 }},
                 "size": self.M}
        
        self.last_query = query
        with open('../settings.json') as f:
            j = load(f)
        
        client = Elasticsearch(j["ElasticSearch"]['es_url'])
        
        response = client.search(index=j["ElasticSearch"]['es_index'], body=query, explain=True)
        
        out = []
        max_score = response['hits']['max_score']
        for hit in response['hits']['hits']:
            title = hit['_source']['metadata']['title'] if 'metadata' in hit['_source'] and 'title' in hit['_source']['metadata'] else ""
            out.append((hit['_id'], hit['_score']/max_score, hit['_source']['metadata']['id'], title))
            
            m = {}
            if len(nested) > 1:
                for s in hit['_explanation']['details']:
                    key = re.findall('metadata.(.*?):', s['details'][0]['description'])[0]
                    m[key] =  s['value'] / max_score
            else:
                s = hit['_explanation']
                key = re.findall('metadata.(.*?):', s['details'][0]['description'])[0]
                m[key] =  s['value'] / max_score
                
            '''
            self.matchings[hit['_id']] = [('{};{}'.format(res['_id'], k),
                                           '{};{}'.format(hit['_id'], k),
                                           v) for k, v in m.items()]
            '''
            self.matchings[hit['_id']] = m
        return out
    
    def get_matching(self, key):
        val = self.matchings.get(key)
        return val if val is not None else {}
