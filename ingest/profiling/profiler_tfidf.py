from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
from filters import fetch_top_k
from time import time
import pandas as pd
from collections import Counter
from json import load

cond = True
while cond:
    # init connection
    with open('../../settings.json') as f:
        j = load(f)
        
    es = Elasticsearch(j["ElasticSearch"]['es_url'])
    es_index = j["ElasticSearch"]['es_index']
    
    q1 = es.search(index=es_index, size=0, 
                   body={"query": {"match" : { "profile.freqs": 'pending'}}})

    
    if q1['hits']['total']['value'] == 0:
        q2 = es.search(index=es_index, size=0, 
                       body = {"query": {"match" : { "profile.status": "done"}}})
        q2 = q2['hits']['total']['value']
        q3 = es.search(index=es_index, size=0, 
                       body = {"query": {"match" : { "profile.freqs": 'done'}}})
        q3 = q3['hits']['total']['value']
        print('Total Profiled Datasets in Lake: {:,}, with tf-idf: {:,}'.format(q2, q3))
        break

    # Get from ES all datasets without profile (NAIVE: checks profile existance afterwards)
    res = es.search(index=es_index, size=100,
                   body={"query":{"match" : { "profile.freqs": "pending"}}})
    todo = 0
    done = 0
    errors = 0
    
    for r in res['hits']['hits']:
        if r['_source']['profile']['freqs'] == 'pending':
            todo += 1
            # Get the id (needed to update doc afterwards)
            doc_id = r['_id']
            # Get the type, to choose the profiler
            doc_type = r['_source']['dtype']
            # Get the path, to be sent to the profiler
            doc_path = r['_source']['path']
    
            # print('--------------------------------------------------------------------------------')
            print('\nDOC: {}\t {} \t {} \t {}'.format(todo, doc_id, doc_type, doc_path))
            print('--------------------------------------------------------------------------------')
            
            
            # Call the profiler
            try:
                for col in r['_source']['profile']['columns']:  
                    w = fetch_top_k(r['_id'], col, k=10)
                    r['_source']['profile']['report']['variables'][int(col)]['freqs'] = w
                r['_source']['profile']['freqs'] = 'done'
                print('DONE')
                done += 1
            except Exception as e:
                r['_source']['profile']['freqs'] = 'error'
                print('ERROR: {}'.format(e))
                errors += 1

            # Update the profile in ES
            es.update(index=es_index, id=doc_id, body={'doc': {'profile': r['_source']['profile']}})                

    print('\n\nTODO: ' + str(todo) + '\t' + 'DONE: ' + str(done) + '\t' + 'ERRORS: ' + str(errors))
