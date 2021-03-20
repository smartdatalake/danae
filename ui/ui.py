#!/usr/bin/env python3
import dash
from dash.dependencies import Input, Output, State
import dash_core_components as dcc
import dash_html_components as html
from methods import fetch_ids, fetch_id, make_general_tab, make_variable_tab

import dash_cytoscape as cyto
from matching import get_graph, graph_style


from styles import style_div, style_logo, active_color
from divs import result_columns
import dash_table
import dash_bootstrap_components as dbc
import json
import requests

host = '0.0.0.0'
port = 9212

app = dash.Dash(__name__, title='DANAE', update_title=None,
                external_stylesheets=[dbc.themes.BOOTSTRAP, 
                                      "https://use.fontawesome.com/releases/v5.7.2/css/all.css"],
)
app.layout = html.Div([
    
    dcc.Store(id='suggested_names'),
    html.Div(children=html.H1(id='title', children='DANAE', style=style_logo), style=style_div),
    html.Div(children=[
        
        html.Div(className='autocomplete', 
                 children=dcc.Input(id='search_bar', type='text', placeholder='Search for dataset...',
                                    debounce=False, autoComplete="off"), style={'width': '60%'}),
        dcc.Input(id='search_bar_2', type='text', placeholder='Search for company...',
                      debounce=False, style={'width': '60%', 'display': 'none'}, autoComplete="off"),
        html.Span(id='search_go', title='Search',  children=[html.I(className="fas fa-search ml-2")], style={'color':active_color}),
        dcc.Store(id='search_selected', data=""),],
         style=style_div),
    html.Div(id='dump1', style={'display':'none'}),
    html.Div(children=[html.H3(children='Profiling'), 
                       html.Span(id='profiling_go', title='Query Profiling',
                                 children=[html.I(className="fas fa-chart-line ml-2")], 
                                 style={'color':active_color, 'margin-top':'7px', 'margin-left':'15px'})], 
             style=style_div),
    dbc.Collapse(id='profiling_collapse', children=[
        html.Div(children=[dcc.Tabs(id='query_dataset_tabs', style = {'margin-top': '10px'}, vertical=True),],
                         id='dataset', style=dict(style_div, **{'display':'none'})),
        html.Hr(),]),
    html.Div(children=[html.H3(children='Weights'), 
                       html.Span(id='weights_go', title='Weights Settings',
                                 children=[html.I(className="fas fa-sliders-h ml-2")], 
                                 style={'color':active_color, 'margin-top':'7px', 'margin-left':'15px'})], 
             style=style_div),    
    dbc.Collapse(id='weights_collapse', children=[
        html.Hr(),]),    
    html.Div(id='submit_div',
             children=[html.Button('Find Similar', id='find', style={'border-radius': '10px', 'background-color': active_color, 'color': '#fff', 'padding': '10px 20px', 'font-size': '20px'})],
             style={'display':'flex', 'justify-content': 'center', 'width': '100%', 'margin-bottom':'20px', 'margin-top':'20px'}),

    dcc.Loading(html.Div(id='results',
             children=[dash_table.DataTable(
                     id='results_list',
                     columns=result_columns,
                     data=[],
                     row_selectable="single",
                     style_as_list_view=True,
                     style_header={'font-size':'1.0em'},
                     style_table={
                         'whiteSpace': 'normal',
                         'height': 'auto',
                         'width': '10%',
                         },
                     style_cell={
                         'whiteSpace': 'normal',
                         'height': 'auto',
                         },  
                     ),
                 dcc.Tabs(id='result_dataset_explore',
                          children = [dcc.Tab(label='Profiling', value='profiling',
                                  children=html.Div(id='result_dataset',
                                   children= dcc.Tabs(id='result_dataset_tabs', vertical=True,
                                                      style = {'width': '50%', 'margin-top': '10px'}), 
                                   style = {'display':'flex', 'justify-content': 'center', 'max-height':'350px'})),
                                  dcc.Tab(label='Content Matching', value='matching',
                                      children=html.Div(cyto.Cytoscape(id='matching', stylesheet=graph_style,        
                                         layout = {'name': 'preset'},
                                         style={'width': '100%', 'height': '800px'},
                                         ))),
                 ])],
             style={'display':'flex'}))
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
    [Output("search_selected", "data"), Output("query_dataset_tabs", "children"), Output("dataset", "style"), ],
    [Input("search_go", "n_clicks")],
    [State("search_bar", "value")]
)
def search_company(n_clicks, search_value):
    ctx = dash.callback_context

    if not ctx.triggered:
        return [dash.no_update]*3
    X = fetch_id(d_title = search_value)
    
    if X is None:
        return [dash.no_update] * 3
    children = make_general_tab(X['_source']['profile']['report']['table'])

    id = X['_id']

    for no, val in enumerate(X['_source']['profile']['report']['variables']):
        children +=  make_variable_tab(val, id, no)
    
    style = {'display':'flex', 'justify-content': 'center', 'max-height':'350px'}
    
    return [X, children, style]

@app.callback(
    [Output("result_dataset_tabs", "children"), Output("matching", "elements")],
    [Input("results_list", "selected_rows")],
    [State("results_list", "data")]
)
def search_result(sel_value, data):
    ctx = dash.callback_context

    if not ctx.triggered:
        return [dash.no_update]*2
    
    
    sel_id = data[sel_value[0]]['result_id']
    
    X = fetch_id(d_id = sel_id)
    
    
    if X is None:
        return [dash.no_update]*2
    children = make_general_tab(X['_source']['profile']['report']['table'])

    id = X['_id']

    for no, val in enumerate(X['_source']['profile']['report']['variables']):
        children +=  make_variable_tab(val, id, no)
    
    elements=get_graph(data[sel_value[0]]['matching'])
    
    return [children, elements]


@app.callback(
    [Output("results_list", "data")],
    [Input("find", "n_clicks")],
    [State("search_selected", "data")]
)
def find_similars(n_clicks, X):
    ctx = dash.callback_context

    if not ctx.triggered:
        return [dash.no_update]
    
    data = json.dumps({"id": [X['_id']]})
    
    result = requests.post('http://localhost:9213/', data=data,
                           headers={'Content-Type':'application/json', 'accept': 'application/json'})
    
    if result.status_code != 200:
        return [dash.no_update]
    
    data = result.json()['pairs']
        
    return [data]


@app.callback(
    Output("weights_collapse", "is_open"),
    [Input("weights_go", "n_clicks")],
    [State("weights_collapse", "is_open")],
)
def weights_collapse(n, is_open):
    if n:
        return not is_open
    return is_open

@app.callback(
    Output("profiling_collapse", "is_open"),
    [Input("profiling_go", "n_clicks")],
    [State("profiling_collapse", "is_open")],
)
def profiling_collapse(n, is_open):
    if n:
        return not is_open
    return is_open


if __name__ == '__main__':
    app.run_server(debug=True, host= host, port=port)
