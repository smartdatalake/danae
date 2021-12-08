import random
from itertools import islice

class Graph:
    
    def __init__(self):
        self.__create()
 
        
    def __create(self):
       self.nodes = {'operative': {'content_query': {'data': {'id': 'content_query', 'label': 'Content Query'}},
                                   'content_total': {'data': {'id': 'content_total', 'label': 'Content Total'}, 'classes': ['triangle'], 'position': {'x': 250, 'y': 300}},
                                   'metadata_query': {'data': {'id': 'metadata_query', 'label': 'Metadata Query'}},
                                   'metadata_total': {'data': {'id': 'metadata_total', 'label': 'Metadata Total'}, 'classes': ['triangle'], 'position': {'x': 250, 'y': 500}},
                                   'total': {'data': {'id': 'total', 'label': 'Total'}, 'position': {'x': 250, 'y': 400}},
                                    },
                      'actual': {
                          }
                      }
        
       self.edges = {'operative':  {'content_total': {'data': {'source': 'content_total', 'target': 'total', 'label': 0.5}},
                                    'metadata_total': {'data': {'source': 'metadata_total', 'target': 'total', 'label': 0.5}}
                                  },
                      'actual': {
                              }
                      }

       self.positions = {'content_query': [(x,y) for x in range(0, 500, 100) for y in range(0, 300, 50)],
                         'metadata_query': [(x,y) for x in range(0, 500, 100) for y in range(600, 800, 50)],
                         'content_result': [(x,y) for x in range(600, 1000, 100) for y in  range(0, 300, 50)],
                         'metadata_result': [(x,y) for x in range(600, 1000, 100) for y in range(600, 800, 50)],
                          }
        
       self.colors = {'Temporal': 'green',
                      'Categorical': 'red',
                      'Numeric': 'blue',
                      'Spatial': 'yellow',
                      'Metadata': 'purple',
                      'Unsupported': 'grey',}      
       
    def __change_positions(self, nodes):
        d = {'content_query': 0, 'metadata_query':0, 'content_result':0, 'metadata_result':0}
        for i in range(len(nodes)):
            p = nodes[i]['data']['parent']
            pos = self.positions[p][d[p]]
            d[p] += 1
            nodes[i]['position'] = {'x': pos[0], 'y': pos[1]}
        return nodes
        
    def create_elements(self):
        elements = []
        elements += list(self.nodes['operative'].values())
        elements += self.__change_positions(list(self.nodes['actual'].values()))
        elements += list(self.edges['operative'].values())
        elements += list(self.edges['actual'].values())
        return elements
        
    def make_dataset_graph(self, X):
        self.R = X['_id']
        self.__create()
        return self.create_elements()
    
    def __get_parent(self, ntype):
        if ntype == 'Metadata':
            return 'metadata'
        elif ntype == 'Result':
            return 'result'
        return 'content'

    def update_nodes(self, selected_nodes):
        new_nodes = {}
        new_edges = {}
        for node, ntype in selected_nodes:
            if node in self.nodes['actual']:
                new_nodes[node] = self.nodes['actual'][node]
                new_edges[node] = self.edges['actual'][node]
            else:
                p = self.__get_parent(ntype)
                new_nodes[node] = {'data': {'id': node, 'label': node.split(';')[1], 'parent': f'{p}_query'},
                                        'position': random_position('CS'),
                                        'classes': self.colors[ntype]}
                new_edges[node] = {'data': {'target': f'{p}_total',
                                            'source': node, 'label': 1.0}}
            
        self.nodes['actual'] = new_nodes
        self.edges['actual'] = new_edges
           
        #item['position'] = random_position('{}{}'.format(x[0],y[0]).upper())
        
        return self.create_elements()


    def update_edges(self, selected_edges, weight):
        for edge in selected_edges:
            if edge['source'].endswith('total'):
                self.edges['operative'][edge['source']]['data']['label'] = float(weight)
            else:
                self.edges['actual'][edge['source']]['data']['label'] = float(weight)
        return self.create_elements()
    
    
    def find_selected_fields(self, type):
        sel_type = '{}_query'.format(type)
        
        total = sum([self.edges['actual'][node]['data']['label'] for node, val in self.nodes['actual'].items()
                     if val['data']['parent'] == sel_type])
        
        out = {}
        for node, val in self.nodes['actual'].items():
            if val['data']['parent'] == sel_type:
                out[node.split(';')[1]] = self.edges['actual'][node]['data']['label'] / total
        return out
    
    
    def find_weight(self, type):
        total = sum([edge['data']['label'] for e, edge in self.edges['operative'].items()
                     if edge['data']['target'] == 'total'])
            
        sel_type = '{}_total'.format(type)
        for e, edge in self.edges['operative'].items():
            if edge['data']['source'] == sel_type:
                return edge['data']['label'] / total
    
    
    def add_matching(self, nodes, matching):
        #if 'content_result' in self.nodes['operative']:
        if hasattr(self,'S'):
            pop_nodes = set()
            for node, data in self.nodes['actual'].items():
                if 'parent' in data['data'] and data['data']['parent'].endswith('result'):
                    pop_nodes.add(node)
            for node in pop_nodes:
                self.edges['actual'].pop(node)
                self.nodes['actual'].pop(node)                
            
        self.nodes['operative']['content_result'] = {'data': {'id': 'content_result', 'label': 'Content Result'}}
        self.nodes['operative']['metadata_result'] = {'data': {'id': 'metadata_result', 'label': 'Metadata Result'}}

        self.S = list(islice(nodes.items(), 1))[0][0].split(';')[0]
        for edge in matching['content']['edges']:
            s, t = edge[0] if edge[0][0].startswith(self.S) else (edge[0][1], edge[0][0])
            
            self.nodes['actual'][s] = {'data': {'id': s, 'label': s.split(';')[1], 'parent': 'content_result'},
                                       'position': random_position('S'),
                                       'classes': self.colors[nodes[s]]}
            
            self.edges['actual'][s] = {'data': {'target': t, 'source': s, 'label': '{:.3f}'.format(edge[2])}}
            
        for field, weight in matching['metadata'].items():
            s = f'{self.S};{field}'
            t = f'{self.R};{field}'
            
            self.nodes['actual'][s] = {'data': {'id': s, 'label': field, 'parent': 'metadata_result'},
                                       'position': random_position('S'),
                                       'classes': self.colors['Metadata']}
            
            self.edges['actual'][s] = {'data': {'target': t, 'source': s, 'label': weight}}            
            
        return self.create_elements()


def random_position_dict(bounds):
    return {'x': random.choice(range(bounds[0][0], bounds[0][1])),
            'y': random.choice(range(bounds[1][0], bounds[1][1]))}

def random_position(option):
    if option == 'CU':
        return random_position_dict(((0,200),(600,800)))
    elif option == 'CS':
        return random_position_dict(((0,200),(200,400)))
    elif option == 'MU':
        return random_position_dict(((700,800),(0,200)))
    elif option == 'MS':
        return random_position_dict(((500,600),(0,200)))
    elif option == 'S':
        return random_position_dict(((0,200),(0,200)))











dataset_graph_style = [
    # Group selectors
    {
        'selector': 'node',
        'style': {
            'content': 'data(label)'
        }
    },

    # Class selectors
    {
        'selector': '.red',
        'style': {
            'background-color': 'red',
            'line-color': 'red'
        }
    },
    {
        'selector': '.green',
        'style': {
            'background-color': 'green',
            'line-color': 'green'
        }
    },
    {
        'selector': '.blue',
        'style': {
            'background-color': 'blue',
            'line-color': 'blue'
        }
    },
    {
        'selector': '.yellow',
        'style': {
            'background-color': 'yellow',
            'line-color': 'yellow'
        }
    },    
    {
        'selector': '.purple',
        'style': {
            'background-color': 'purple',
            'line-color': 'purple'
        }
    },    
    {
        'selector': '.grey',
        'style': {
            'background-color': 'grey',
            'line-color': 'grey'
        }
    },        
    {
        'selector': '.triangle',
        'style': {
            'shape': 'triangle'
        }
    },
    {
        'selector': '.rrectangle',
        'style': {
            'shape': 'round rectangle'
        }
    },
    {
      'selector': "edge[label]",
      'css': {
        "label": "data(label)",
        "text-rotation": "autorotate",
        "text-margin-x": "0px",
        "text-margin-y": "-20px"
      }
    },    
    
]

