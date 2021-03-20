import csv
import chardet
import gzip
import pandas as pd
import re
from pandas_profiling import ProfileReport
import json
from collections import MutableMapping, Counter

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
    #d['value_counts'] = var_val['value_counts']
    stats = []
    for key, val in var_val.items():
        #if key =='value_counts':
        #    continue
        stats.append({'key': key, 'value': str(val)})
    d['stats'] = stats
    d['name'] = var_name
    return d

def get_profile(df):
    p = delete_keys_from_dict(json.loads(ProfileReport(df, minimal=True, progress_bar=False).to_json()))
    p['variables'] = [prepare_variable(key, val) for key, val in p['variables'].items()]
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
    return modified_dict

def search_field(d, key):
    for val in d:
        if val['key'] == key:
            return val['value']
        
def quantiles(col):
    return [{'key': '{}%'.format(int(p * 100)), 'value': str(col.quantile(p))}
            for p in {0.25, 0.50, 0.75}]
