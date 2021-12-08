from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search, Q
from elasticsearch.helpers import scan
import pandas as pd
import requests
import json
import math
import sys
from time import sleep
from collections import Counter
from word_aggregator import WordAggregator

from index import RTree
from intervaltree import Interval, IntervalTree
from datetime import datetime
from dateutil import parser
from scipy.spatial.distance import euclidean
from itertools import islice
import heapq
from numpy import std
from math import log
from json import load

import networkx as nx

class ContentSearcher:
    
    def __init__(self, decay=0.01):
        #self.dateTree= RTree('date', 2, True)
        self.dateTree= RTree('date', 7)
        #self.dateTree = IntervalTree()
        self.numTree= RTree('num', 7)
        self.catTree= RTree('cat', 50)
        self.spatTree = RTree('spat', 2, flat=True)
        #return
        self.wa = WordAggregator()
        self.matchings = {}
        self.cached_col_vecs = {}
        self.cached_kths = {}
        self.decay = decay
        
        self.keys = {}

    def __insert_item(self, dtype, id, x):
        if x is None:
            return
        
        if dtype == 'Numeric':
            self.numTree.insert(id, x)
        elif dtype == 'Temporal':
            self.dateTree.insert(id, x)
        elif dtype  == 'Categorical':
            self.catTree.insert(id, x)
        elif dtype  == 'Spatial':
            self.spatTree.insert(id, x)
            
            
    def __dist(self, x, y, dtype, simple=True):
        if simple:
            return euclidean(x, y)
        else:
            if dtype  == 'Numeric':
                return euclidean(x, y[:7])
            elif dtype  == 'Temporal':
                #return abs(x[0] - y[0]) + abs(x[1] - y[2])
                return euclidean(x, y[:7])
            elif dtype == 'Categorical':
                return euclidean(x, y[:50])
            elif dtype  == 'Spatial':
                return euclidean(x, y)
            
    def __search_item(self, dtype, x, L, M, score=True, w=1):
        if x is None:
            return None, None
        
        if score:
            if dtype  == 'Numeric':
                res = self.numTree.nearest(x, M, objects=True)
            elif dtype  == 'Temporal':
                res = self.dateTree.nearest(x, M, objects=True)
            elif dtype == 'Categorical':
                res = self.catTree.nearest(x, M, objects=True)  
            elif dtype  == 'Spatial':
                res = self.spatTree.nearest(x, M, objects=True)  
                
            res = {r[0]: self.__dist(x, r[1], dtype, False) for r in res}  

            L2 = min(L, len(res))
            vals = sorted(res.values())
            while vals[L2-1] == 0 and L2 < len(vals):
                L2 += 1

            kth = vals[L2-1]    
            if kth == 0.0:  #if ranked list has no non-zero elements
                kth = 0.000000000001
            
              
            #kth = sorted(res.values())[-k if k < len(res) else 0]
            #if kth == 0:
            #    kth = sys.float_info[3]
            
            #res = {key: val / kth for key, val in res.items() }
            #res = {key: math.exp(-decay * (val / kth)) for key, val in res.items() }
            res = [(key, w * math.exp(-self.decay * (val / kth))) for key, val in res.items()]
            res = sorted(res, key=lambda x: -x[1])
            return (res, kth)
        else:
            if dtype  == 'Numeric':
                res = self.numTree.nearest(x, M)
            elif dtype  == 'Temporal':
                res = self.dateTree.nearest(x, M)
            elif dtype == 'Categorical':
                res = self.catTree.nearest(x, M)
            elif dtype  == 'Spatial':
                res = self.spatTree.nearest(x, M)
            return (res, None)
            
    def __get_columns(self, dtype, S):
        if dtype  == 'Numeric':
            return self.numTree.get_columns(S)
        elif dtype  == 'Temporal':
            return self.dateTree.get_columns(S)
        elif dtype == 'Categorical':
            return self.catTree.get_columns(S)
        elif dtype  == 'Spatial':
            return self.spatTree.get_columns(S)


    def __prepare_num(self, col):
        percs = [0 for i in range(7)]
        p = {'min':0, '5%':1, '25%':2, '50%':3, '75%':4, '95%':5, 'max':6}
        for field in col['stats']:
            if field['key'] in p:
                percs[p[field['key']]] = float(field['value'])
        return percs
            
    
    def __prepare_cat(self, col):
        if col is None or 'freqs' not in col:
            return None
        emb = self.wa.transform_sentence(col['freqs'])
        return emb
    
    def __prepare_spat(self, col):
        bounds = [0 for i in range(4)]
        b = {'x_min':0, 'y_min':1, 'x_max':2, 'y_max':3}
        for field in col['stats']:
            if field['key'] in b:
                bounds[b[field['key']]] = float(field['value'])
        return bounds    
            
    def __prepare_date(self, col):
        percs = [0 for i in range(7)]
        p = {'min':0, '5%':1, '25%':2, '50%':3, '75%':4, '95%':5, 'max':6}
        for field in col['stats']:
            if field['key'] in p:
                percs[p[field['key']]] = (parser.parse(field['value']).replace(tzinfo=None) - datetime.utcfromtimestamp(0)).total_seconds()
        return percs
    
    def __prepare_col(self, col):
        if col['type'] == 'Numeric':
            return self.__prepare_num(col)
        elif col['type'] == 'Temporal':
            return self.__prepare_date(col)
        elif col['type'] == 'Categorical':
            return self.__prepare_cat(col)
        elif col['type']  == 'Spatial':        
            return self.__prepare_spat(col)
        else:  #Variable.S_TYPE_UNSUPPORTED,Variable.TYPE_BOOL 
            return None
            
    def __insert_dataset(self, r):
        if 'profile' in r['_source'] and 'report' in r['_source']['profile']:
            rid = r['_id']
            m_id = r['_source']['metadata']['id'] 
            m_title = r['_source']['metadata']['title'] if 'title' in r['_source']['metadata'] else ""
            
            self.keys[rid] = (m_id, m_title)
            
            for no, col in enumerate(r['_source']['profile']['report']['variables']):
                #val = vars[str(no)] if str(no) in vars else None
                x = self.__prepare_col(col)
                
                self.__insert_item(col['type'], '{};{}'.format(rid, col['name']), x)
                
    def __search_dataset(self, r, fields, L, M, weight=False):
        out = []
        if 'profile' not in r['_source'] or 'report' not in r['_source']['profile']:
            return out
        for no, col in enumerate(r['_source']['profile']['report']['variables']):
            if col['name'] not in fields:
                continue
            
            w = fields[col['name']]
            
            x = self.__prepare_col(col)
            res, kth = self.__search_item(col['type'], x, L, M, w=w)
            #if weight == False and res is not None and len(res) > 0:
            #    w = std(list(zip(*res))[1])
            out.append(((col['type'], x, col['name']), res, kth, w))
        return out
    
    
    def __init_step(self, ranked, i):
        sum_i = 0
        for col in range(len(ranked)):
            if ranked[col] is None:
                continue
            if i < len(ranked[col]):
                sum_i += ranked[col][i][1]
            else:
                sum_i += ranked[col][-1][1]
        return sum_i
    
    def train(self):
        with open('../settings.json') as f:
            j = load(f)
            
        client = Elasticsearch(j["ElasticSearch"]['es_url'], timeout=200)
        
        
        # Fetching ids of records with profile.report
        query = {"_source": ["_id"], "query": { "exists": {"field": "profile.report"}}}

        data = client.search(index=j["ElasticSearch"]['es_index'], scroll='2m',
                             size=1000, body=query)

        sid, scroll_size = data['_scroll_id'], len(data['hits']['hits'])

        ids = []

        while scroll_size > 0:
            ids += [hit['_id'] for hit in data['hits']['hits']]
            print('{:,}\r'.format(len(ids)), end='')
            data = client.scroll(scroll_id=sid, scroll='2m')
            sid, scroll_size = data['_scroll_id'], len(data['hits']['hits'])
              
        
        print('Total ids: {:,}'.format(len(ids)))

        #Iterate on ids, to request each of them from ES
        for i, rid in enumerate(ids):
            if i % 200 == 0:
                print(f'{i}\r', end='')
            query = {"_source": ["metadata.title", "metadata.id", "profile.columns", "profile.report.variables"],
                     "query": { "match": {"_id":rid} }, "size": 1}


            response = scan(client, index=j["ElasticSearch"]['es_index'], query=query)
    
            for i, r in enumerate(response):
                break
            self.__insert_dataset(r)
        #query = {"_source": ["metadata.title", "metadata.id", "profile.columns", "profile.report.variables"],
        #         "query": { "match_all": {} }, "size": 5}
            

    def search(self, res, fields, L=1, M=1):
        if 'profile' not in res['_source'] or 'report' not in res['_source']['profile']:
            return None, None
        
        col_vectors, ranked, kths, weights = zip(*self.__search_dataset(res, fields, L, M))
        print(weights)
        
        r_len = len([v for v in col_vectors if v[1] is not None])
        
        
        lens = [len(nn) for nn in ranked if nn is not None]
        ranked_d = [dict(r) if r is not None else None for r in ranked]
        if len(lens) == 0:
            return None, None
        len_max_ranked = max(lens)
        
        h = []
        
        #Rid = res['_source']['metadata']['id']
        Rid = res['_id']
        Rtitle = res['_source']['metadata']['title']
        self.cached_col_vecs[Rid] = col_vectors
        self.cached_kths[Rid] = kths
        
        cands = set()
        for i in range(len_max_ranked): #iteration for rows of ranked lists (horizontal)
            sum_i = self.__init_step(ranked, i)
            if len(h) == L and heapq.nsmallest(1, h)[0].score >= sum_i:
                out = [hit.scale(r_len) for hit in heapq.nlargest(L, h)]
                return sorted(out, key=lambda x: -x[1]), weights
            
            for col in range(len(ranked)): #iteration of each ranked list (vertical)
                if ranked[col] is None or i >= len(ranked[col]):
                    continue
                
                S, score = ranked[col][i]
                S = S.split(';')[0]

                if S in cands or S == Rid:
                    continue
                
                cands.add(S)
                edges = []
            
                for no, (type_col, vec_col, r_col) in enumerate(col_vectors): #iteration of cols for specific entity
                     if vec_col is None: # no vector for some reason
                         continue
                 
                     s_cols = self.__get_columns(type_col, S)
                     if s_cols is None:    # no similar type from S to r
                         continue
                     for s_col in s_cols:
                         sid = '{};{}'.format(S, s_col)
                         w = weights[no]
                         if ranked[no] is not None and sid in ranked_d[no]:
                             sim = ranked_d[no][sid]
                             edges.append(('{};{}'.format(Rid, r_col), sid, sim, sim/w))
                         else:
                             dist = self.__dist(vec_col, s_cols[s_col], type_col)
                             #edge = w * math.exp(-self.decay * (dist / kths[no]))
                             #edges.append(('{};{}'.format(Rid, r_col), sid, edge))
                             
                             sim = math.exp(-self.decay * (dist / kths[no]))
                             edges.append(('{};{}'.format(Rid, r_col), sid, w*sim, sim))
            
                G = nx.Graph()  
                # G.add_weighted_edges_from(edges)
                for e in edges:
                    G.add_edge(e[0], e[1], weight=e[2], original=e[3])
                 
                for (type_col, vec_col, r_col) in col_vectors:
                     n = '{};{}'.format(Rid, r_col)
                     if n not in G:
                         continue
                     nx.set_node_attributes(G, {n:{'type':type_col}})
                     nx.set_node_attributes(G, {n1:{'type':type_col} for n1 in G.neighbors(n)})
                 
                matching = nx.max_weight_matching(G)
                #if len(matching) < 2:
                #    continue
                 
                self.matchings[S] = {'nodes': dict(G.nodes(data=True)),
                                     'edges': [(e,G.edges[e]['weight'], G.edges[e]['original']) for e in matching],
                                     'partitions': {Rid: Rtitle, 
                                                    S: self.keys[S][1]}}
                
                 
                score = sum([G.edges[e]['weight'] for e in matching])
                 
                
                if len(h) < L:
                    heapq.heappush(h, Item(S, self.keys[S], score))
                else:
                    heapq.heappushpop(h, Item(S, self.keys[S], score))

        out = [hit.scale(r_len) for hit in heapq.nlargest(L, h)]
        #return dict([hit.scale(r_len) for hit in h])
        return (sorted(out, key=lambda x: -x[1]), weights)
  
    
    def search_missing(self, S, Rid, weights):
        if Rid not in self.cached_col_vecs:
            return 0
        
        col_vectors = self.cached_col_vecs[Rid] 
        kths = self.cached_kths[Rid]
        
        r_len = len([v for v in col_vectors if v[1] is not None])
            
        edges = []
    
        for no, (type_col, vec_col, r_col) in enumerate(col_vectors):
             if vec_col is None: # no vector for some reason
                 continue
         
             s_cols = self.__get_columns(type_col, S)
             if s_cols is None:    # no similar type from S to r
                 continue
             for s_col in s_cols:
                 sid = '{};{}'.format(S, s_col)
                 w = weights[no]
                 dist = self.__dist(vec_col, s_cols[s_col], type_col)
                 # edge = w*math.exp(-self.decay * (dist / kths[no]))
                 sim = math.exp(-self.decay * (dist / kths[no]))
                 edges.append(('{};{}'.format(Rid, r_col), sid, w*sim, sim))
    
        G = nx.Graph()  
        # G.add_weighted_edges_from(edges)
        for e in edges:
            G.add_edge(e[0], e[1], weight=e[2], original=e[3])        
         
        for (type_col, vec_col, r_col) in col_vectors:
             n = '{};{}'.format(Rid, r_col)
             if n not in G:
                 continue
             nx.set_node_attributes(G, {n:{'type':type_col}})
             nx.set_node_attributes(G, {n1:{'type':type_col} for n1 in G.neighbors(n)})
         
        matching = nx.max_weight_matching(G)

         
        self.matchings[S] = {'nodes': dict(G.nodes(data=True)),
                             'edges': [(e,G.edges[e]['weight'], G.edges[e]['original']) for e in matching]}
        
         
        score = sum([G.edges[e]['weight'] for e in matching])
                 
        return score / r_len
    
    def clear_cache(self, Rid):
        del self.cached_col_vecs[Rid] 
        del self.cached_kths[Rid]
        
    
    def get_matching(self, key):
        val = self.matchings.get(key)
        return val if val is not None else {}    
        
class Item:
    def __init__(self, S, meta, score):
        self.S = S
        self.m_id = meta[0]
        self.m_title = meta[1]
        self.score = score

    def __lt__(self, other):
       if self.score < other.score:
           return True
       elif self.score == other.score:
           if self.S < other.S:
               return True
       return False
   
    def __repr__(self):
        return "({}, {})".format(self.S, self.score)
    
    # def scale(self, n):
    #     return (self.S, self.score/n, self.m_id, self.m_title)
    
    def scale(self, n):
        return (self.S, self.score, self.m_id, self.m_title)
