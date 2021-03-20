import pandas as pd
import numpy as np
from collections import Counter

class WordAggregator:
    
    def __init__(self, path='embeddings/glove.6B.50d.txt'):
        with open(path) as f:
            embs = f.readlines()
        embs = [emb[:-1].split(' ') for emb in embs]
        self.d = len(embs[0])-1
        self.embs = {emb[0]: np.array([float(v) for v in emb[1:]]) for emb in embs}

    def transform_sentence(self, sentence, sep=' ', opt=True, topk=10):
        if sentence is None:
            return None
        
        if type(sentence) == str:
            s = set(sentence.split(sep))
        else:
            s = sentence
            
        if opt:
            s, freq = zip(*Counter(s).most_common(topk))
 
        t = np.zeros(self.d)
        i=0
        for w in s:
            if w in self.embs:
                i += 1
                t += self.embs[w]
            
        if i == 0: #no word in dict
            return None
        else:
            return list(t/i)
        
