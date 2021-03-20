import numpy as np
import pandas as pd
import math
import requests

url = 'http://localhost:9211/publish'

# Get the data from somewhere

data_catalogue = '/mnt/data/eu_data_portal/metadata/total_3.csv'
files_path = '/mnt/data/eu_data_portal/files/'

datasets = pd.read_csv(data_catalogue, sep=';').to_dict(orient='records')

submitted = 0

for r in datasets:
    # prepare the description
    d = {}
    if type(r['title']) == str:
        d['title'] = r['title']
    else:
        d['title'] = r['id']
    d['path'] = files_path + r['id'] + '.csv.gz'
    d['type'] = 'TABULAR'
    if type(r['keywords']) == str:
        d['keywords'] = r['keywords']
    if type(r['description']) == str:
        d['description'] = r['description']
    d['metadata'] = {}
    for key in r:
        if type(r[key]) != float or not math.isnan(r[key]):
            d['metadata'][key] = r[key]
    
    # submit it
    res = requests.post(url, json = d)
    
    submitted += 1
    
    if submitted % 200 == 0:
        print(submitted)
    
print('Datasets submitted for publishing: ' + str(submitted))
