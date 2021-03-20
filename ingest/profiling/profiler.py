from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
from filters import get_encoding, get_separator, collect_header, is_csv_readable, get_num_rows, get_profile, search_field, quantiles
from time import time
import pandas as pd

while True:
    # init connection
    es = Elasticsearch()
    es_index = 'danae-eodp'
    
    
    types = {'cat': set(['Variable.TYPE_CAT', 'Categorical']),
             'num': set(['Variable.TYPE_NUM', 'Numeric']), 
             'date': set(['Variable.TYPE_DATE', 'DateTime'])}
    

    q1 = es.search(index=es_index, size=0, 
                   body={"query": {"bool" : { "must_not": {"exists": {"field": "profile"}}}}},)
    
    if q1['hits']['total']['value'] == 0:
        q2 = es.search(index=es_index, size=0, 
                       body = {"query": { "exists": {"field": "profile"}}})
        q3 = es.search(index=es_index, size=0, 
                       body = {"query": { "exists": {"field": "profile.report"}}})
        print('Total Datasets in Lake: {:,}, profiled: {:,}'.format(q2['hits']['total']['value'], q3['hits']['total']['value']))
        break

    # Get from ES all datasets without profile (NAIVE: checks profile existance afterwards)
    res = es.search(index=es_index, size=100,
                   body={"query": {"bool" : { "must_not": {"exists": {"field": "profile"}}}}},)
    todo = 0
    done = 0
    errors = 0
    for r in res['hits']['hits']:
        if 'profile' not in r['_source']:
            todo += 1
            # Get the id (needed to update doc afterwards)
            doc_id = r['_id']
            # Get the type, to choose the profiler
            doc_type = r['_source']['dtype']
            # Get the path, to be sent to the profiler
            doc_path = r['_source']['path']
    
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
                    
                    for col in df.columns:
                        if df[col].dtype == 'object':
                            try:
                                df[col] = pd.to_datetime(df[col])
                            except ValueError:
                                pass
                
                    profile['report'] = get_profile(df)
                    
                
                    profile['columns'] = {}
                    for i, col in enumerate(df):
                        stats = profile['report']['variables'][i]['stats']
                        v_type = search_field(stats, 'type')            
                        p_missing = float(search_field(stats, 'p_missing'))
                        
                        if v_type in types['cat'] and p_missing < 0.1:
                            profile['columns'][i] = df[col].astype(str).str.cat(sep=' ')
                            
                        elif v_type in types['date']:
                            profile['report']['variables'][i]['stats'] += quantiles(df.iloc[:,i])
                
                profile['time'] = time()
                
                # Update the profile in ES
                es.update(index=es_index, id=doc_id, body={'doc': {'profile': profile}})
                print('DONE')
                done += 1
            except Exception as e:
                print('ERROR: {}'.format(e))
                es.update(index=es_index, id=doc_id, body={'doc': {'profile': {'status': 'error'}}})
                errors += 1
    
    print('\n\nTODO: ' + str(todo) + '\t' + 'DONE: ' + str(done) + '\t' + 'ERRORS: ' + str(errors))
