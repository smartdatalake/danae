from combined_search import CombinedSearcher
from time import time
import pandas as pd
import flask
from flask import request


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
    ids = request.json['id']
    
    print('Starting queries...')

    print(ids)
    t1 = time()
    out = cs.search(ids, M=30, L=15, k=5)
    out = sorted(out, key = lambda x: -x['overall_score'])
    t2 = time()
    print('Time elapsed: {:.2f} sec'.format(t2-t1))
    
    return {'pairs': out}
app.run(host='0.0.0.0', port=9213)    
