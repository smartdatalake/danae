from rtree import index
from bidict import bidict

class RTree:

    def __init__(self, name, no, flat=False):
        idx_properties = index.Property()
        idx_properties.dimension = no
        idx_properties.overwrite = False
        
        self.idx = index.Index('idx/index_{}_{}'.format(name, no), properties=idx_properties)
        self.d = bidict()
        
        self.flat = flat
        
        self.inv = {}
    
    def insert(self, key, val):
        self.d[key] = len(self.d)
        
        data, col = key.split(";", 1)
        if data not in self.inv:
            self.inv[data] = {}
        self.inv[data][col] = val
        
        #obj = [val[0], 0, val[1], 0] if self.flat else val + val
        obj = val if self.flat else val + val
        self.idx.insert(self.d[key], obj)
    
    #def nearests(self, X, k):
    #    return [self.nearest(x, k) for x in X]
    
    def get_columns(self, S):
        if S in self.inv:
            return self.inv[S]
    
    def nearest(self, x, k, objects=False):
        #obj = [x[0], 0, x[1], 0] if self.flat else x*2
        obj = x if self.flat else x*2
        if objects:
            return [(self.d.inv[r.id], r.bbox) for r in self.idx.nearest(obj, k, objects)]
        else:
            return [self.d.inv[r] for r in self.idx.nearest(obj, k)]
            
    def save_model(self, path):
        self.idx.close()