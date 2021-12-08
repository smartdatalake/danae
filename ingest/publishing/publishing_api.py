#!/usr/bin/env python3
from flask import Flask, request
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Index, Document, Text, Keyword, Date, Object
from json import load


class DataAssetDescription(Document):
    title = Text()
    path = Keyword()
    dtype = Keyword()
    description = Text()
    keywords = Text()
    time_created = Date()
    metadata = Object
    profile = Object
    insights = Object

with open('../../settings.json') as f:
    j = load(f)

client = Elasticsearch(j["ElasticSearch"]['es_url'])
index_name = j["ElasticSearch"]['es_index']

es_index = Index(index_name, using=client)

if not es_index.exists():
    DataAssetDescription.init(index=index_name, using=client)
    print('Mapping created!')

app = Flask('Data Publishing API')

@app.route('/publish', methods=['POST'])
def publish():
    # parse the request and create the document
    req_data = request.json
    #doc = DataAssetDescription(metadata=req_data['metadata'])
    doc = DataAssetDescription(metadata=req_data['metadata'], profile=req_data['profile'])
    doc.title = req_data['title']
    doc.path = req_data['path']
    doc.dtype = req_data['type']
    if 'description' in req_data:
        doc.description = req_data['description']
    if 'keywords' in req_data:
        doc.keywords = req_data['keywords']
    if 'time_created' in req_data:
        doc.time_created = req_data['time_created']
    doc.save(using=client, index=index_name)
    return "Published"

if __name__ == '__main__':
    with open('../../settings.json') as f:
        j = load(f)
    app.run(debug=True, host= '0.0.0.0', port=j['ports']['publish'])
