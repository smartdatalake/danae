import numpy as np

class WordAggregator:
    
    def __init__(self, path='embeddings/glove.6B.50d.txt'):
        with open(path) as f:
            embs = f.readlines()
        embs = [emb[:-1].split(' ') for emb in embs]
        self.d = len(embs[0])-1
        #self.embs = pd.DataFrame(embs).set_index(0).astype(float)
        self.embs = {emb[0]: np.array([float(v) for v in emb[1:]]) for emb in embs}

    def transform_sentence(self, terms):
        if terms is None or len(terms) == 0:
            return None
        
        out = np.zeros(self.d)
        
        for term in terms:
            word = term['key']
            if word in self.embs:
                out += self.embs[word]
            
        return list(out/len(terms))
        
