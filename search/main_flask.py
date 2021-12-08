#!/usr/bin/env python3
from combined_search import CombinedSearcher
from time import time
import flask
from flask import request
from json import load


app = flask.Flask(__name__)
#app.config["DEBUG"] = True

print('Building indices...')
t1 = time()
cs = CombinedSearcher()
t2 = time()
print('Time elapsed: {:.2f} sec'.format(t2-t1))

print('Starting ingestion...')
t1 = time()
cs.train()
t2 = time()
print('Time elapsed: {:.2f} sec'.format(t2-t1))

@app.route('/', methods=['POST'])
def search():
    ids = request.json['query']
    params = request.json['params']
    
    print('Starting queries...')

    
    t1 = time()
    M, L, k = params.values()
    out = cs.search(ids, M=M, L=L, k=k)
    out = sorted(out, key = lambda x: -x['overall_score'])
    t2 = time()
    print('Time elapsed: {:.2f} sec'.format(t2-t1))
    
    return {'pairs': out}

if __name__ == '__main__':
    with open('../settings.json') as f:
        j = load(f)
    app.run(host= '0.0.0.0', port=j['ports']['simsearch'])
