import csv
import chardet
import gzip
import pandas as pd
import re
from pandas_profiling import ProfileReport
from json import load, loads
from collections import MutableMapping, Counter
from elasticsearch import Elasticsearch
from math import log

def get_encoding(file):
    """ Returns the encoding of the file """
    test_str = b''
    number_of_lines_to_read = 50
    count = 0
    with gzip.open(file,'rb') as f:
        line = f.readline()
        while line and count < number_of_lines_to_read:
            test_str = test_str + line
            count += 1
            line = f.readline()
        result = chardet.detect(test_str)
    return result['encoding']

def get_separator(file, encoding):
    try:
        r = pd.read_csv(file, encoding=encoding, nrows=10, sep = None,
            iterator = True, engine='python', header=None)
        return r._engine.data.dialect.delimiter
        
    except:
        return None    

def collect_header(file, sep, encoding):
    try:
        r = pd.read_csv(file, encoding=encoding, nrows=1, sep=sep, header=None)
        r = ';'.join(str(h) for h in r.values[0])
        r = re.sub('\n|\r', '', r)
        return r
    except:
        return None
    
def is_csv_readable(header):
    try:
        if '<!DOCTYPE' in header or header.startswith("{") or header.startswith("["):
            return False
        return True
    except:
        return False
    
def get_num_rows(file, sep, encoding):
    df = pd.read_csv(file, encoding=encoding, sep=sep, header=None, chunksize=100000)
    rows = 0
    for chunk in df:
        rows += chunk.shape[0]
    return rows

def add_key_value(d, key, val):
    d[key] = val
    return d


def prepare_variable(var_name, var_val):
    d= {}
    stats = []
    dtype = ""
    for key, val in var_val.items():
        if key == 'type':
            dtype = str(val)
            continue
        stats.append({'key': key, 'value': str(val)})
        
    d['stats'] = stats
    d['type'] = transform_field(dtype)
    d['name'] = var_name
    return d
'''
def prepare_variable(var_name, var_val):
    d= {}
    #d['value_counts'] = var_val['value_counts']
    stats = {}
    for key, val in var_val.items():
        #if key =='value_counts':
        #    continue
        stats[key] = val
    d['stats'] = stats
    d['name'] = var_name
    return d
'''

def get_profile(df):
    p = delete_keys_from_dict(loads(ProfileReport(df, minimal=True, progress_bar=False).to_json()))
    p['variables'] = [prepare_variable(key, val) for key, val in p['variables'].items()]
    #p['variables'] = [add_key_value(val, "name", key) for key, val in p['variables'].items()]
    return p

def delete_keys_from_dict(dictionary):
    keys_set = set(['histogram', 'histogram_data', 'scatter_data', 'value_counts',
                    'histogram_frequencies', 'package', 'value_counts_without_nan', 'first_rows'])

    modified_dict = {}
    for key, value in dictionary.items():
        if key not in keys_set:
            if isinstance(value, MutableMapping):
                modified_dict[key] = delete_keys_from_dict(value)
            else:
                modified_dict[key] = value  # or copy.deepcopy(value) if a copy is desired for non-dicts.
        #elif key == 'value_counts':
        #    modified_dict[key] = [{'word':key, 'freq':val} for (key, val) in Counter(value).most_common(10)]
    return modified_dict

def search_field(d, key):
    for val in d:
        if val['key'] == key:
            return val['value']
        
def transform_field(field):
    if field in ['Variable.TYPE_CAT', 'Categorical']:
        return 'Categorical'
    elif field in ['Variable.TYPE_NUM', 'Numeric']:
        return 'Numeric'
    elif field in ['Variable.TYPE_DATE', 'DateTime']:
        return 'Temporal'
    else:
        return 'Unsupported'
        
def quantiles(col):
    return [{'key': '{}%'.format(int(p * 100)), 'value': str(col.quantile(p))}
            for p in {0.05, 0.25, 0.50, 0.75, 0.95}]

def add_spatial_stats(var, values):
    keys = ['x_min','x_max','y_min','y_max']
    return {'stats': [{ 'key': k, 'value': str(v)} for k,v in zip(keys, values)] + var['stats'],
            'type': 'Spatial', 'name': 'Location'}

def fetch_top_k(id, col, k=10, tf=True):
    with open('../../settings.json') as f:
        j = load(f)
        
    client = Elasticsearch(j["ElasticSearch"]['es_url'], timeout=20)
    
    field = f'profile.columns.{col}'
    r = client.termvectors(j["ElasticSearch"]['es_index'], id=id, fields=[field],
                           offsets=False, positions=False, term_statistics=True)
    
    N = r['term_vectors'][field]['field_statistics']['doc_count']
     
    if tf:
        freqs = {k: v['term_freq'] for k,v in r['term_vectors'][field]['terms'].items()}
    else:
        freqs = {k: (v['term_freq'] / v['ttf']) * (log(N / v['doc_freq']))
                  for k,v in r['term_vectors'][field]['terms'].items()}
    
    c = Counter(freqs)
    #return c.most_common(k)  
    #return [k for k, v  in c.most_common(k)]
    return [{"key":key, "value":val} for key, val in c.most_common(k)]