import requests
import json
from content_search import ContentSearcher
from metadata_search import MetadataSearcher
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search, Q
from elasticsearch.helpers import scan
import heapq

from itertools import islice

client = Elasticsearch()
es_index = 'danae-eodp'
             
class CombinedSearcher:
    
    def __init__(self):
        self.cs = ContentSearcher()
        self.ms = MetadataSearcher(100)
        
    def train(self):
        self.cs.train()
        
    def __score(self, c, m, w_c=0.5, w_m=0.5):
        return c * w_c + m * w_m
    
    def __inner_search(self, res, k, L, M, decay):
        Rid = res['_id']
        content_matches = self.cs.search(res, L, M)
        cd = {t[0]: t[1] for t in content_matches}
        if not content_matches:
            return None
        
        
        R_kwd = res['_source']['metadata']['keywords'] if 'keywords' in res['_source']['metadata'] else ''
        R_title = res['_source']['metadata']['title'] if 'title' in res['_source']['metadata'] else ''
        R_desc = res['_source']['metadata']['description'] if 'description' in res['_source']['metadata'] else ''
        
        metadata_matches = self.ms.search(R_kwd, R_title, R_desc)
        md = {t[0]: t[1] for t in metadata_matches}
        
        p = max(len(content_matches), len(metadata_matches))
        h = []
        
        UB = 2.0
        c_i, m_i= 0, 0
        examined = set([Rid])
        print(Rid)
        for i in range(p):
            UB = self.__score(content_matches[c_i][1] if len(cd) > 0 else 0, 
                              metadata_matches[m_i][1] if len(md) > 0 else 0)
            if c_i < len(content_matches):
                S, c_score, S_id, S_title = content_matches[c_i] 
                if S not in examined:
                    examined.add(S)
                    
                    if len(md) > 0:
                        if S in md:
                            m_score = md[S]
                        else:   #calculate now
                            #m_score = self.ms.search_missing(S, res['_source']['keywords'])
                            m_score = 0
                    else:
                        m_score = 0
                        
                    if c_i < len(content_matches) - 1:
                        c_i += 1
                    UB = self.__score(content_matches[c_i][1], metadata_matches[m_i][1] if len(md) > 0 else 0)
    
                    item = Item(S, S_id, S_title, c_score, m_score, self.__score(c_score, m_score), self.cs.matchings[S])
                    if len(h) < k:
                        heapq.heappush(h, item)
                    else:
                        heapq.heappushpop(h, item)     
                else:
                    if c_i < len(content_matches) - 1:
                        c_i += 1                        
            
            if len(h) == k and heapq.nsmallest(1, h)[0].o_score >= UB:
                return [hit.to_dict() for hit in heapq.nlargest(len(h), h)]

            if m_i < len(metadata_matches):            
                S, m_score, S_id, S_title = metadata_matches[m_i] 
                if S not in examined:
                    examined.add(S)
                
                    if len(cd) > 0:
                        if S in cd:
                            c_score = cd[S]
                        else:   #calculate now
                            c_score = self.cs.search_missing(S, Rid)
                    else:
                        c_score = 0                            
                    if m_i < len(metadata_matches) - 1:
                        m_i += 1
                    UB = self.__score(content_matches[c_i][1] if len(cd) > 0 else 0, metadata_matches[m_i][1])
                    
                    item = Item(S, S_id, S_title, c_score, m_score, self.__score(c_score, m_score), self.cs.matchings[S])
                    if len(h) < k:
                        heapq.heappush(h, item)
                    else:
                        heapq.heappushpop(h, item)
                else:
                    if m_i < len(metadata_matches) - 1:
                        m_i += 1
                        
            if len(h) == k and heapq.nsmallest(1, h)[0].o_score >= UB:
                return [hit.to_dict() for hit in heapq.nlargest(len(h), h)]
         
        return [hit.to_dict() for hit in heapq.nlargest(len(h), h)]
        
    
    def search(self, ids, k=5, L=10, M=30, decay=0.01):
        
        out = []
        if ids is None:
            query = {"_source": ["metadata.id", "profile.columns", "profile.report.variables"],
                     "query": { "match_all": {} }, "size": 5}
            response = scan(client, index=es_index, query=query)
    
            for i, res in enumerate(response):
                if i % 10 == 0:
                    print("Record {}\r".format(i), end='')
                    out += self.__inner_search(res, k, L, M, decay)

        else: 
            for i, id in enumerate(ids):
                if i % 10 == 0:
                    print("Record {}\r".format(i), end='')
                query = {"query": { "match": {"_id": id} }, "size": 1}
                response = client.search(index=es_index, body=query)
                res = list(islice(response['hits']['hits'], 1))[0]
                
                out += self.__inner_search(res, k, L, M, decay)
        return out


class Item:
    def __init__(self, S, S_id, S_title, c_score, m_score, o_score, matching):
        self.S = S
        self.S_id = S_id
        self.S_title = S_title
        self.c_score = c_score
        self.m_score = m_score
        self.o_score = o_score
        self.matching = matching

    def __lt__(self, other):
       if self.o_score < other.o_score:
           return True
       elif self.o_score == other.o_score:
           if self.S < other.S:
               return True
       return False
   
    def __repr__(self):
        return "({}, {}, {}, {})".format(self.S, self.c_score, self.m_score, self.o_score)
    
    def to_dict(self):
        return {'_id': self.S, 'result_id': self.S_id, 'result_title': self.S_title,
                'overall_score': self.o_score, 'content_score': self.c_score,
                'metadata_score': self.m_score, 'matching': self.matching}
