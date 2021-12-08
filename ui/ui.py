#!/usr/bin/env python3
import dash
from dash.dependencies import Input, Output, State
import dash_core_components as dcc
import dash_html_components as html
from methods import fetch_ids, fetch_ids_by_title, fetch_id, retrieve_info, transform_dataset, retrieve_general, retrieve_names, find_var_in_lists

import dash_cytoscape as cyto
from dataset_graph import dataset_graph_style, Graph


from styles import style_div, style_logo, active_color, style_sdl, style_div_none
from divs import tabs
#from eodp_dash_web_methods import fetch_query_from_elasticsearch, fetch_query_from_yggdrasil, fetch_query_from_simjoin, transform_data, transform_general_data
import dash_bootstrap_components as dbc
from json import load, dumps
import requests

app = dash.Dash(__name__, title='DANAE', update_title=None,
                external_stylesheets=[dbc.themes.BOOTSTRAP, 
                                      "https://use.fontawesome.com/releases/v5.7.2/css/all.css"],
)
app.layout = html.Div([
    
    dcc.Store(id='suggested_names'),
    dcc.Store(id='edge_weight'),
    dcc.Store(id='query_selected', data=""),
    dcc.Store(id='result_selected', data=""),
    html.Div(children=html.H1(id='title', children='DANAE', style=style_logo), style=style_div),
    html.Div(children=[
        
        html.Div(className='autocomplete', 
                 children=dcc.Input(id='search_bar', type='text', placeholder='Search for dataset...',
                                    debounce=False, autoComplete="off"), style={'width': '60%'}),
        dcc.Input(id='search_bar_2', type='text', placeholder='Search for company...',
                      debounce=False, style={'width': '60%', 'display': 'none'}, autoComplete="off"),
        html.Span(id='search_go', title='Search',  children=[html.I(className="fas fa-search ml-2")], style={'color':active_color}),
        ],
         style=style_div),
    html.Div(id='dump1', style={'display':'none'}),
    html.Div(id='dump2', style={'display':'none'}),
    dbc.Modal([dbc.ModalHeader("Edit Weight"),
                dbc.ModalBody(dcc.Input(id='weight_input', type='text', pattern = '\d\.\d*?')),
                dbc.ModalFooter([
                    dbc.Button("Edit", id="modal_btn_edit"),
                    dbc.Button("Cancel", id="modal_btn_cancel")]
                ),
            ],
            id="modal",size="sm"),
    tabs,
    
    html.Div(html.Button('Find Similar', id='find', 
                         style={'border-radius': '10px', 'background-color': active_color, 'color': '#fff', 'padding': '10px 20px', 'font-size': '20px'}),
             style={'display':'flex', 'justify-content': 'center'}),
    html.Div (id='dataset', style=style_div_none, children = [
            cyto.Cytoscape(id='dataset_graph', stylesheet=dataset_graph_style,
                           boxSelectionEnabled=True,
                           #layout={'name': 'grid', 'cols': 2},
                           layout = {'name': 'preset'},
                           autoRefreshLayout = True,
                           #layout = {'name' : 'circle'},
                           style={'width': '50%', 'height': '800px'},
                           ),
            ]),
 
    ])


app.clientside_callback(
    """
    function(data) {
        if (data === undefined)
            return {};
        
        autocomplete(document.getElementById("search_bar"), data);
        
        return {};
    }
    """,
    Output("dump1", "children"),
    [Input("suggested_names", "data")]
) 


@app.callback(
    [Output("suggested_names", "data")],
    [Input("search_bar", "value")],
) 
def suggest_names(value):
    ctx = dash.callback_context
    if not ctx.triggered:
        return [dash.no_update]
    
    if len(value) < 3:
        return [[]]
        
    return [[[lab,val] for val, lab in fetch_ids(value)]]
    #return [[html.Option(value=lab, label=lab, id=val) for val, lab in fetch_ids(value)]]


@app.callback(
    [Output("search_bar", "value")],
    [Input("search_bar_2", "value")],
) 
def choose_name(value):
    ctx = dash.callback_context
    if not ctx.triggered:
        return [dash.no_update]
    
    return [value]


@app.callback(
    Output("query_list", "data"),
    Input("search_go", "n_clicks"),
    State("search_bar", "value")
)
def search_query_dataset(btn1, search_value):
    ctx = dash.callback_context
    if not ctx.triggered:
        return dash.no_update

    return fetch_ids_by_title(search_value)
        


@app.callback(
    [Output("query_selected", "data"), Output("query_general", "children"),
     Output("dataset_graph", "elements"), Output("dataset", "style")]+
    [Output(f"query_data_{i}", "data") for i in range(1, 6)] +
    [Input("query_list", "selected_rows"), Input("edge_weight", "data"),
     Input("result_selected", "data")] +
    [Input(f"query_data_{i}", "selected_rows") for i in range(1, 6)],
    [State("dataset_graph", "selectedNodeData"), State("dataset_graph", "selectedEdgeData"),
     State("results_list", "selected_rows"), State("results_list", "data"),
     State("query_selected", "data"), State("query_list", "data"),] +
    [State(f"query_data_{i}", "data") for i in range(1, 6)]
)
def select_query_dataset(query_selected, weight, Y, qs1, qs2, qs3, qs4, qs5,
                         selected_nodes, selected_edges,
                         result_selected, result_data, X, query_data,
                         qd1, qd2, qd3, qd4, qd5):

    ret = [dash.no_update] * 9
    
    ctx = dash.callback_context
    if not ctx.triggered:
        return ret

    prop = ctx.triggered[0]['prop_id'].split('.')[0]
    if prop == 'query_list':
        sel_id = query_data[query_selected[0]]['_id']
        X = fetch_id(sel_id)
        
        if X is None:
            return ret
        
        ret[0] = transform_dataset(X)
        ret[1] = retrieve_general(ret[0])
        ret[2] = g.make_dataset_graph(ret[0])
        #ret[3] = {'display':'block'}
        ret[3] = {'display':'flex', 'justify-content': 'center'}
        ret[4:10] = list(retrieve_names(ret[0]).values())
        
        #return [X, general_children, elements, style] + list(names.values()) + [[]*5]
        return ret
    elif prop.startswith('query_data'):

        Rid = X['_id']
        selected = []
        types = ['Content', 'Categorical', 'Numeric', 'Temporal', 'Spatial', 'Metadata']
        for i, t in enumerate(types):
            if i==0:
                continue
            sel = locals()[f'qs{i}']
            if sel is None:
                continue
            d = locals()[f'qd{i}']
            for n in sel:
                selected.append(('{};{}'.format(Rid, d[n]['col_id']), t))
        
        ret[2] = g.update_nodes(selected)
        #return [dash.no_update]*2 + [elements] + [dash.no_update] * 6 + [dash.no_update] * 5
        return ret
                
    elif prop == 'edge_weight':
        ret[2] = g.update_edges(selected_edges, weight)
        #return [dash.no_update]*2 + [elements, dash.no_update]  + [dash.no_update] * 5
        return ret
    elif prop == 'result_selected':
        sel_id = result_data[result_selected[0]]['_id']
    
        nodes = {'{};{}'.format(sel_id, n['name']): n['type'] for n in Y['_source']['profile']['report']['variables']}
        ret[2]  = g.add_matching(nodes, result_data[result_selected[0]]['matching'])
        #return [dash.no_update]*2 + [elements, dash.no_update]  + [dash.no_update] * 5
        return ret

@app.callback(
    [Output(f"query_toggle_{i}", "children") for i in range(0, 6)],
    [Input(f"query_data_{i}", "data") for i in range(1, 6)] +
    [Input(f"query_data_{i}", "selected_rows") for i in range(1, 6)],
    [State(f"query_toggle_{i}", "children") for i in range(0, 6)],
)
def update_card_header(qd1, qd2, qd3, qd4, qd5,
                       qs1, qs2, qs3, qs4, qs5,
                       h0, h1, h2, h3, h4, h5):
    ctx = dash.callback_context

    if not ctx.triggered:
        return [dash.no_update]*6
    
    header = [h0, h1, h2, h3, h4, h5]
    qds = [[], qd1, qd2, qd3, qd4, qd5]
    qss = [[], qs1, qs2, qs3, qs4, qs5]
    
    for trig in ctx.triggered:
        prop = int(trig['prop_id'].split('.')[0].split('_')[-1])
        name = header[prop].split(' ')[0]
        total = len(qds[prop])
        
        if trig['prop_id'].endswith('data'):
            curr = 0
        elif trig['prop_id'].endswith('selected_rows'):  
            curr = len(qss[prop])

        header[prop] = '{} {}/{}'.format(name, curr, total)
        
    curr = sum([len(q) for q in qss[:-1]])        
    total = sum([len(q) for q in qds[:-1]])
    header[0] = 'Content {}/{}'.format(curr, total)
    return header



     
@app.callback(
    [Output("result_selected", "data"), Output("result_general", "children")] +
    [Output(f"result_toggle_{i}", "children") for i in range(0, 6)] +
    [Output(f"result_data_{i}", "data") for i in range(1, 6)],
    [Output(f"result_data_{i}", "selected_rows") for i in range(1, 6)],
    [Input("results_list", "selected_rows"), Input("query_list", "selected_rows")],
    [State("results_list", "data")],
)
def search_result(result_sel, query_sel, data):
    ctx = dash.callback_context

    if not ctx.triggered:
        return [dash.no_update]*18
    
    trig = ctx.triggered[0]
    
    if trig['prop_id'].startswith('results_list'):
        sel_id = data[result_sel[0]]['_id']
        
        X = fetch_id(sel_id)
        X = transform_dataset(X)
        
        if X is None:
            return [dash.no_update]*18
    
        general_children = retrieve_general(X)
        names = list(retrieve_names(X).values())
        
        sel = [[] for name in names]
        for e in data[result_sel[0]]['matching']['content']['edges']:
            var = e[0][0] if e[0][0].startswith(sel_id) else e[0][1]
            var = var.split(';')[1]
            l, index = find_var_in_lists(var, names)
            sel[l].append(index)

        for var in data[result_sel[0]]['matching']['metadata']:
            l, index = find_var_in_lists(var, names)
            sel[l].append(index)
        
        header = ['Categorical', 'Numeric', 'Temporal', 'Spatial', 'Metadata']
        header = ['{} {}/{}'.format(h, len(s), len(n)) for h,s,n in zip(header, sel, names)]
        
        
        sel_cont = sum([len(s) for s in sel[:-1]])
        total_cont = sum([len(n) for n in names[:-1]])
        header.insert(0, 'Content {}/{}'.format(sel_cont, total_cont))
        
        return [X, general_children] + header + names + sel
    elif trig['prop_id'].startswith('query_list'):
        return [[]] * 18


@app.callback(
    [Output("results_list", "data")],
    [Input("find", "n_clicks")],
    [State("query_selected", "data"), State("dataset_graph", "elements")]
)
def find_similars(n_clicks, X, elements):
    ctx = dash.callback_context

    if not ctx.triggered:
        return [dash.no_update]
    

    content_cols = g.find_selected_fields("content")
    content_weights = g.find_weight('content') if len(content_cols) > 0 else 0
    metadata_cols = g.find_selected_fields("metadata")
    metadata_weights = g.find_weight('metadata') if len(metadata_cols) > 0 else 0

    data = dumps({"query": {X['_id'] : 
                                 {'content' : {'columns': content_cols,
                                               'weight': content_weights},
                                  'metadata': {'fields': metadata_cols,
                                               'weight': metadata_weights}
                                  }},
                  "params": {"M":100, "L":50, "k": 15}
                         })
        
    with open('../settings.json') as f:
        j = load(f)

    url = 'http://localhost:{}'.format(j['ports']['simsearch'])   
     
    result = requests.post(url, data=data,
                           headers={'Content-Type':'application/json', 'accept': 'application/json'})
    
    if result.status_code != 200:
        return [dash.no_update]
    
    data = result.json()['pairs']
  
    return [data]



@app.callback(
    [Output("query_field", "children"), Output("result_field", "children")],
    [Input("dataset_graph", "selectedNodeData")] +
    [Input(f"query_data_{i}", "selected_rows") for i in range(1, 6)],
    [State("query_selected", "data"), State("result_selected", "data")] +
    [State(f"query_data_{i}", "data") for i in range(1, 6)]
)
def select_node(nodeData, qs1, qs2, qs3, qs4, qs5, X, Y, qd1, qd2, qd3, qd4, qd5):
    ctx = dash.callback_context

    ret = [dash.no_update]*2
    if not ctx.triggered:
       return ret 
    
    if ctx.triggered[-1]['prop_id'].startswith('dataset_graph'):
        if len(nodeData) == 0 or 'parent' not in nodeData[0]:
            return ret
        
        if nodeData[0]['parent'].endswith('result'):
            col = retrieve_info(Y, nodeData[0]['label'], nodeData[0]['parent'].startswith('content'))
            ret[1] = col
        else:
            col = retrieve_info(X, nodeData[0]['label'], nodeData[0]['parent'].startswith('content'))
            ret[0] = col
    elif ctx.triggered[-1]['prop_id'].startswith('query_data'):
        prop = int(ctx.triggered[-1]['prop_id'].split('.')[0].split('_')[-1])
        if len(locals()[f'qs{prop}']) == 0:
            return ret
        no_col = locals()[f'qs{prop}'][-1]
        data_col = locals()[f'qd{prop}']
        col = retrieve_info(X, data_col[no_col]['col_id'], prop < 4)
        ret[0] = col
    return ret
    
@app.callback(
    [Output(f"query_data_{i}", "selected_rows") for i in range(1, 6)],
    [Input("btn_select", "n_clicks"), Input("btn_unselect", "n_clicks"),
     Input("query_list", "selected_rows")],
    [State(f"query_data_{i}", "data") for i in range(1, 6)]
)
def change_selected(btn1, btn2, q_sel, qd1, qd2, qd3, qd4, qd5):
    ctx = dash.callback_context

    if not ctx.triggered:
        return [dash.no_update]*5
    
    if ctx.triggered[0]['prop_id'].startswith('btn_select'):
        out = []
        for i in range(1,6):
            out.append(list(range(len(locals()[f'qd{i}']))))
        return out
    elif ctx.triggered[0]['prop_id'].startswith('btn_unselect'):
        return [[]]*5
    elif ctx.triggered[0]['prop_id'].startswith('query_list'):
        return [[]]*5
    
@app.callback(
    [Output("modal", "is_open"), Output("weight_input", "value"), Output("edge_weight", "data")],
    [Input("dataset_graph", "selectedEdgeData"), Input("modal_btn_cancel", "n_clicks"),
     Input("modal_btn_edit", "n_clicks")],
    [State("modal", "is_open"), State("weight_input", "value")],
) 
def edit_edge(selected, btn1, btn2, is_open, weight):
    if selected is None or len(selected) == 0:
        return [dash.no_update] * 3

    ctx = dash.callback_context

    if not ctx.triggered:
        return [dash.no_update] * 3

    prop = ctx.triggered[0]['prop_id'].split('.')[0]

    if prop == 'dataset_graph':
        return [True, selected[0]['label'], dash.no_update]
    elif prop == 'modal_btn_cancel':
        return [False, dash.no_update, dash.no_update]
    elif prop == 'modal_btn_edit':
        return [False, dash.no_update, weight]
    
    return [dash.no_update]*3

@app.callback(
    [Output(f"query_collapse_{i}", "is_open") for i in range(0, 6)],
    [Input(f"query_toggle_{i}", "n_clicks") for i in range(0, 6)],
    [State(f"query_collapse_{i}", "is_open") for i in range(0, 6)],
)
def toggle_query_accordion(n0, n1, n2, n3, n4, n5, is_open0, is_open1, is_open2, is_open3, is_open4, is_open5):
    ctx = dash.callback_context

    res = [dash.no_update]*6

    if not ctx.triggered:
        return res
    
    bid = int(ctx.triggered[0]["prop_id"].split(".")[0].split('_')[-1])
    if locals()[f'n{bid}']:
        res[bid] = not locals()[f'is_open{bid}']

    return res

@app.callback(
    [Output(f"result_collapse_{i}", "is_open") for i in range(0, 6)],
    [Input(f"result_toggle_{i}", "n_clicks") for i in range(0, 6)],
    [State(f"result_collapse_{i}", "is_open") for i in range(0, 6)],
)
def toggle_result_accordion(n0, n1, n2, n3, n4, n5, is_open0, is_open1, is_open2, is_open3, is_open4, is_open5):
    ctx = dash.callback_context

    res = [dash.no_update]*6

    if not ctx.triggered:
        return res
    
    bid = int(ctx.triggered[0]["prop_id"].split(".")[0].split('_')[-1])
    if locals()[f'n{bid}']:
        res[bid] = not locals()[f'is_open{bid}']

    return res

@app.callback(
    Output("query_collapse_preview", "is_open"),
    [Input("query_toggle_preview", "n_clicks")],
    [State("query_collapse_preview", "is_open")],
)
def query_preview_toggle_collapse(n, is_open):
    if n:
        return not is_open
    return is_open

@app.callback(
    Output("result_collapse_preview", "is_open"),
    [Input("result_toggle_preview", "n_clicks")],
    [State("result_collapse_preview", "is_open")],
)
def result_preview_toggle_collapse(n, is_open):
    if n:
        return not is_open
    return is_open


if __name__ == '__main__':
    g = Graph()
    with open('../settings.json') as f:
        j = load(f)
    app.run_server(debug=True, host= '0.0.0.0', port=j['ports']['ui'])    
