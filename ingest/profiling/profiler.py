from elasticsearch import Elasticsearch
from filters import get_encoding, get_separator, collect_header, is_csv_readable, get_num_rows, get_profile, search_field, quantiles, add_spatial_stats
from time import time
import pandas as pd
from collections import Counter
from json import load
import numpy as np

cond = True
while cond:
    # init connection
    with open('../../settings.json') as f:
        j = load(f)
        
    es = Elasticsearch(j["ElasticSearch"]['es_url'])
    es_index = j["ElasticSearch"]['es_index']
    
    q1 = es.search(index=es_index, size=0, 
                   body={"query": {"match" : { "profile.status": "pending"}}},)
    
    if q1['hits']['total']['value'] == 0:
        q2 = es.search(index=es_index, size=0, 
                       body = {"query": { "exists": {"field": "profile"}}})
        q3 = es.search(index=es_index, size=0, 
                       body = {"query": {"match" : { "profile.status": "done"}}})
        print('Total Datasets in Lake: {:,}, profiled: {:,}'.format(q2['hits']['total']['value'], q3['hits']['total']['value']))
        break

    # Get from ES all datasets without profile (NAIVE: checks profile existance afterwards)
    res = es.search(index=es_index, size=100,
                   body={"query": {"match" : { "profile.status": "pending"}}},)
    todo = 0
    done = 0
    errors = 0
    
    for r in res['hits']['hits']:
        if r['_source']['profile']['status'] == 'pending':
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
                profile = {}
                
                #get encoding of file
                profile['encoding'] = get_encoding(doc_path)
                if profile['encoding'] is None:
                    raise ValueError("Error in Encoding.")
                
                #get separator of file
                profile['separator'] = get_separator(doc_path, profile['encoding'])
                if profile['separator'] is None:
                    raise ValueError("Error in Separator.")
        
                #get header, columns and if csv_readable
                profile['header'] = collect_header(doc_path, profile['separator'], profile['encoding'])
                profile['is_csv_readable'] = is_csv_readable(profile['header'])
                if not profile['is_csv_readable']:
                    raise ValueError("Error in Parsing.")
                profile['num_columns'] = len(profile['header'].split(';'))
        
                num_keywords = 0
                if 'keywords' in r['_source']:
                    num_keywords = len(r['_source']['keywords'].split(','))
                profile['num_keywords'] = num_keywords
                
                profile['num_rows'] = get_num_rows(doc_path, profile['separator'], profile['encoding'])
                
                if profile['num_rows'] <= 100000:
                    df = pd.read_csv(doc_path, encoding=profile['encoding'], sep=profile['separator'], header=0)
                    
                    drop_col = []
                    for no, col in enumerate(df.columns):
                        if df[col].dtype == 'object':
                            try:
                                df[col] = pd.to_datetime(df[col])
                            except ValueError:
                                pass
                            
                        if col.lower() in ['long', 'lng', 'longitude']:
                            values = df[col] if df[col].dtype == np.float64 else df[col].str.replace(',', '.')
                            min_y, max_y = pd.to_numeric(values, errors='coerce').apply([min, max]).values
                            print(min_y, max_y)
                            drop_col.append((col, no))
                        if col.lower() in ['latt', 'lat', 'lattitude']:
                            values = df[col] if df[col].dtype == np.float64 else df[col].str.replace(',', '.')
                            min_x, max_x = pd.to_numeric(values, errors='coerce').apply([min, max]).values
                            print(min_x, max_x)
                            drop_col.append((col, no))
                    
                    #sort to get first the furthest column
                    drop_col = sorted(drop_col, key=lambda x: -x[1])
                        
                    #remove the furthest column
                    if len(drop_col) > 0:
                        df.drop(drop_col[0][0], axis=1, inplace=True)
                
                    profile['report'] = get_profile(df)
                    if len(drop_col) > 0:
                        #profile['report']['variables'].append(add_spatial_stats([min_x, max_x, min_y, max_y]))
                        profile['report']['variables'][drop_col[1][1]] = add_spatial_stats(profile['report']['variables'][drop_col[1][1]], [min_x, max_x, min_y, max_y])
                
                    c = Counter()
                    profile['columns'] = {}
                    for i, col in enumerate(df):
                        stats = profile['report']['variables'][i]['stats']
                        v_type = profile['report']['variables'][i]['type']
                        p_missing = float(search_field(stats, 'p_missing'))
                        c.update([v_type])
                        
                        if v_type == 'Categorical' and p_missing < 0.1:
                            profile['columns'][i] = df[col].astype(str).str.cat(sep=' ')
                            
                        elif v_type == 'Temporal':
                            profile['report']['variables'][i]['stats'] += quantiles(df.iloc[:,i])
                    profile['report']['table']['types'] = dict(c)
                
                profile['time'] = time()
                
                # Update the profile in ES
                profile['status'] = 'done'
                es.update(index=es_index, id=doc_id, body={'doc': {'profile': profile}})
                print('DONE')
                done += 1
            except Exception as e:
                print('ERROR: {}'.format(e))
                es.update(index=es_index, id=doc_id, body={'doc': {'profile': {'status': 'error'}}})
                errors += 1
    
    print('\n\nTODO: ' + str(todo) + '\t' + 'DONE: ' + str(done) + '\t' + 'ERRORS: ' + str(errors))
