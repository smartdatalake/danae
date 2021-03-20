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

import networkx as nx

client = Elasticsearch()
es_index = 'danae-eodp'

class ContentSearcher:
    
    def __init__(self, decay=0.01):
        self.dateTree= RTree('date', 3)
        self.numTree= RTree('num', 3)
        self.catTree= RTree('cat', 50)
        self.wa = WordAggregator()
        self.matchings = {}
        self.cached_col_vecs = {}
        self.cached_kths = {}
        self.decay = decay
        
        self.types = {'cat': set(['Variable.TYPE_CAT', 'Categorical']),
                      'num': set(['Variable.TYPE_NUM', 'Numeric']), 
                      'date': set(['Variable.TYPE_DATE', 'DateTime'])}
        
        self.keys = {}

    def __insert_item(self, dtype, id, x):
        if x is None:
            return
        
        if dtype in self.types['num']:
            self.numTree.insert(id, x)
        elif dtype in self.types['date']:
            self.dateTree.insert(id, x)
        elif dtype  in self.types['cat']:
            self.catTree.insert(id, x)
            
            
    def __dist(self, x, y, dtype, simple=True):
        if simple:
            if dtype  in self.types['num']:
                return euclidean(x, y)
            elif dtype  in self.types['date']:
                return euclidean(x, y)
            elif dtype in self.types['cat']:
                return euclidean(x, y)
        else:
            if dtype  in self.types['num']:
                return euclidean(x, y[:3])
            elif dtype  in self.types['date']:
                return euclidean(x, y[:3])
            elif dtype in self.types['cat']:
                return euclidean(x, y[:50])            

            
    def __search_item(self, dtype, x, L, M, score=True):
        if x is None:
            return None, None
        
        if score:
            if dtype  in self.types['num']:
                res = self.numTree.nearest(x, M, objects=True)
            elif dtype  in self.types['date']:
                res = self.dateTree.nearest(x, M, objects=True)
            elif dtype in self.types['cat']:
                res = self.catTree.nearest(x, M, objects=True)         
                
            res = {r[0]: self.__dist(x, r[1], dtype, False) for r in res}  

            L2 = min(L, len(res))
            vals = sorted(res.values())
            while vals[L2-1] == 0 and L2 < len(vals):
                L2 += 1

            kth = vals[L2-1]    
            if kth == 0.0:  #if ranked list has no non-zero elements
                kth = 0.000000000001
            
            res = [(key, math.exp(-self.decay * (val / kth))) for key, val in res.items()]
            res = sorted(res, key=lambda x: -x[1])
            return (res, kth)
        else:
            if dtype  in self.types['num']:
                res = self.numTree.nearest(x, M)
            elif dtype  in self.types['date']:
                res = self.dateTree.nearest(x, M)
            elif dtype in self.types['cat']:
                res = self.catTree.nearest(x, M)
            return (res, None)
            
    def __get_columns(self, dtype, S):
        if dtype  in self.types['num']:
            return self.numTree.get_columns(S)
        elif dtype  in self.types['date']:
            return self.dateTree.get_columns(S)
        elif dtype in self.types['cat']:
            return self.catTree.get_columns(S)


    def __prepare_num(self, col):
        percs = [0 for i in range(3)]
        for field in col['stats']:
            if field['key'] == '25%':
                percs[0] = float(field['value'])
            if field['key'] == '50%':
                percs[1] = float(field['value'])
            if field['key'] == '75%':
                percs[2] = float(field['value'])
        return percs
            
    
    def __prepare_cat(self, val):
        #return
        emb = self.wa.transform_sentence(val)
        return emb
            
    def __prepare_date(self, col):
        interval = [0 for i in range(3)]
        for field in col['stats']:
            if field['key'] == '25%':
                interval[0] = (parser.parse(field['value']).replace(tzinfo=None) - datetime.utcfromtimestamp(0)).total_seconds()
            if field['key'] == '50%':
                interval[1] = (parser.parse(field['value']).replace(tzinfo=None) - datetime.utcfromtimestamp(0)).total_seconds()                
            if field['key'] == '75%':
                interval[2] = (parser.parse(field['value']).replace(tzinfo=None) - datetime.utcfromtimestamp(0)).total_seconds()
        return interval        
    
    def __prepare_col(self, id, col, val):
        for field in col['stats']:
            if field['key'] != 'type':
                continue
            
            if field['value']  in self.types['num']:
                return (field['value'], self.__prepare_num(col), col['name'])
            elif field['value']  in self.types['date']:
                return (field['value'], self.__prepare_date(col), col['name'])
            elif field['value'] in self.types['cat']:
                return (field['value'], self.__prepare_cat(val), col['name'])
            else:  #Variable.S_TYPE_UNSUPPORTED,Variable.TYPE_BOOL 
                return (field['value'], None, col['name'])
            
    def __insert_dataset(self, r):
        if 'profile' in r['_source'] and 'report' in r['_source']['profile']:
            id = r['_id']
            m_id = r['_source']['metadata']['id'] 
            m_title = r['_source']['metadata']['title'] if 'title' in r['_source']['metadata'] else ""
            vars = r['_source']['profile']['columns']
            self.keys[id] = (m_id, m_title)
            
            for no, col in enumerate(r['_source']['profile']['report']['variables']):
                val = vars[str(no)] if str(no) in vars else None
                type, x, name = self.__prepare_col(id, col, val)
                
                self.__insert_item(type, '{};{}'.format(id, name), x)
                
    def __search_dataset(self, r, L, M):
        out = []
        if 'profile' in r['_source'] and 'report' in r['_source']['profile']:
            id = r['_id']
            vars = r['_source']['profile']['columns']
            for no, col in enumerate(r['_source']['profile']['report']['variables']):
                val = vars[str(no)] if str(no) in vars else None
                
                col_vector = self.__prepare_col(id, col, val)
                res, kth = self.__search_item(col_vector[0], col_vector[1], L, M)
                out.append((col_vector, res, kth))
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
    
    def __fetch_top_k(self, id, col, k=10):
        field = 'profile.columns.{}'.format(col)
        r = client.termvectors('danae-eodp', id=id, fields=[field], offsets=False, positions=False)
        if len(r['term_vectors']) == 0:
            return None
        freqs = {k: v['term_freq'] for k,v in r['term_vectors'][field]['terms'].items()}
        c = Counter(freqs)
        return c.most_common(k)      
                    
    def train(self):
        query = {"_source": ["metadata.title", "metadata.id", "profile.columns", "profile.report.variables"],
                 "query": { "match_all": {} }, "size": 5}
        response = scan(client, index=es_index, query=query)
    
        for i, r in enumerate(response):
            self.__insert_dataset(r)
            

    def search(self, res, L=1, M=1):
        if 'profile' not in res['_source'] or 'report' not in res['_source']['profile']:
            return None
        
        col_vectors, ranked, kths = zip(*self.__search_dataset(res, L, M))
        
        r_len = len([v for v in col_vectors if v[1] is not None])
        
        
        lens = [len(nn) for nn in ranked if nn is not None]
        ranked_d = [dict(r) if r is not None else None for r in ranked]
        if len(lens) == 0:
            return None
        len_max_ranked = max(lens)
        
        h = []
        
        Rid = res['_id']
        self.cached_col_vecs[Rid] = col_vectors
        self.cached_kths[Rid] = kths
        
        cands = set()
        for i in range(len_max_ranked):
            sum_i = self.__init_step(ranked, i)
            if len(h) == L and heapq.nsmallest(1, h)[0].score >= sum_i:
                out = [hit.scale(r_len) for hit in heapq.nlargest(L, h)]
                return sorted(out, key=lambda x: -x[1])
            
            for col in range(len(ranked)):
                if ranked[col] is None or i >= len(ranked[col]):
                    continue
                
                S, score = ranked[col][i]
                S = S.split(';')[0]

                if S in cands or S == Rid:
                    continue
                
                cands.add(S)
                edges = []
            
                for no, (type_col, vec_col, r_col) in enumerate(col_vectors):
                     if vec_col is None: # no vector for some reason
                         continue
                 
                     s_cols = self.__get_columns(type_col, S)
                     if s_cols is None:    # no similar type from S to r
                         continue
                     for s_col in s_cols:
                         sid = '{}_{}'.format(S, s_col)
                         if ranked[no] is not None and sid in ranked_d[no]:
                             edges.append(('{}_{}'.format(Rid, r_col), sid, ranked_d[no][sid]))
                         else:
                             dist = self.__dist(vec_col, s_cols[s_col], type_col)
                             edge = math.exp(-self.decay * (dist / kths[no]))
                             edges.append(('{}_{}'.format(Rid, r_col), sid, edge))
            
                G = nx.Graph()  
                G.add_weighted_edges_from(edges)
                 
                for (type_col, vec_col, r_col) in col_vectors:
                     n = '{}_{}'.format(Rid, r_col)
                     if n not in G:
                         continue
                     nx.set_node_attributes(G, {n:{'type':type_col}})
                     nx.set_node_attributes(G, {n1:{'type':type_col} for n1 in G.neighbors(n)})
                 
                matching = nx.max_weight_matching(G)
                 
                self.matchings[S] = {'nodes': dict(G.nodes(data=True)),
                                     'edges': [(e,G.edges[e]['weight']) for e in matching]}
                
                 
                score = sum([G.edges[e]['weight'] for e in matching])
                 
                
                if len(h) < L:
                    heapq.heappush(h, Item(S, self.keys[S], score))
                else:
                    heapq.heappushpop(h, Item(S, self.keys[S], score))

        out = [hit.scale(r_len) for hit in heapq.nlargest(L, h)]
        return sorted(out, key=lambda x: -x[1])
  
    
    def search_missing(self, S, Rid):
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
                 sid = '{}_{}'.format(S, s_col)
                 
                 dist = self.__dist(vec_col, s_cols[s_col], type_col)
                 edge = math.exp(-self.decay * (dist / kths[no]))
                 edges.append(('{}_{}'.format(Rid, r_col), sid, edge))
    
        G = nx.Graph()  
        G.add_weighted_edges_from(edges)
         
        for (type_col, vec_col, r_col) in col_vectors:
             n = '{}_{}'.format(Rid, r_col)
             if n not in G:
                 continue
             nx.set_node_attributes(G, {n:{'type':type_col}})
             nx.set_node_attributes(G, {n1:{'type':type_col} for n1 in G.neighbors(n)})
         
        matching = nx.max_weight_matching(G)

         
        self.matchings[S] = {'nodes': dict(G.nodes(data=True)),
                             'edges': [(e,G.edges[e]['weight']) for e in matching]}
        
         
        score = sum([G.edges[e]['weight'] for e in matching])
                 
        return score / r_len
    
    def clear_cache(self, Rid):
        del self.cached_col_vecs[Rid] 
        del self.cached_kths[Rid]
        
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
    
    def scale(self, n):
        return (self.S, self.score/n, self.m_id, self.m_title)
