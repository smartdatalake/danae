import dash_cytoscape as cyto
            
     


def get_graph(graph):
    r_id, s_id = graph['edges'][0][0][0].split('_', 1)[0], graph['edges'][0][0][1].split('_', 1)[0]

    colors = {'Variable.TYPE_DATE': 'green',
              'Variable.TYPE_CAT': 'red',
              'Variable.TYPE_NUM': 'blue',
              'DateTime': 'green',
              'Categorical': 'red',
              'Numeric': 'blue',}
    
    elements = [ {'data': {'id': r_id, 'label': r_id}},
                 {'data': {'id': s_id, 'label': s_id}}]
    
    visited = set()
    
    y = 75
    for ((source, target), weight) in sorted(graph['edges'], key=lambda x: -x[1]):
        if source.startswith(s_id):
            source, target = target, source

        p, l = source.split('_', 1)
        elements.append({'data': {'id': source, 'label': l, 'parent': p},
                         'position': {'x': 75, 'y': y},
                         'classes': colors[graph['nodes'][source]['type']]})

        p, l = target.split('_', 1)
        elements.append({'data': {'id': target, 'label': l, 'parent': p},
                         'position': {'x': 500, 'y': y},
                         'classes': colors[graph['nodes'][target]['type']]})
        
        elements.append({'data': {'source': source, 'target': target, 'label': round(weight, 2)}})
        visited.update([source, target])
        y += 50
        
    r_y,  s_y = y, y    

    for node, attr in graph['nodes'].items():
        if node in visited:
            continue
        
        if node.startswith(r_id):
            x = 75
            r_y += 50
            y = r_y
        else:
            x = 500
            s_y += 50
            y = s_y
        color = colors[attr['type']]
        
        p, l = node.split('_', 1)
            
        elements.append({'data': {'id': node, 'label': l, 'parent': p}, 
                         'position': {'x': x, 'y': y},
                         'classes': color})
    
    return elements
    


graph_style = [
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
        'selector': '.triangle',
        'style': {
            'shape': 'triangle'
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

