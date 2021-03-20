from flask import Flask, request
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Index, Document, Text, Keyword, Date, Object


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


client = Elasticsearch()
index_name = 'danae-eodp'

es_index = Index(index_name, using=client)

if not es_index.exists():
    DataAssetDescription.init(index=index_name, using=client)
    print('Mapping created!')

app = Flask('Data Publishing API')

@app.route('/publish', methods=['POST'])
def publish():
    # parse the request and create the document
    req_data = request.json
    doc = DataAssetDescription(metadata=req_data['metadata'])
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
    app.run(debug=True, host= '0.0.0.0', port=9211)
