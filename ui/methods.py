from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search, Q
from elasticsearch.helpers import scan
from itertools import islice
import dash_core_components as dcc
import dash_html_components as html
import dash_table
import pandas as pd
import plotly.express as px
from styles import style_sdl, style_sdl_light
from collections import Counter
import dash 
import dash_bootstrap_components as dbc
from math import log
from dateutil.parser import parse
import folium
from shapely.geometry import box
from json import load


def fetch_ids(kwd):
    if kwd is None or kwd == '':
        return None

    kwd = kwd.lower()
    
    query = {"track_scores":"true", "_source": ["title","metadata.id"], "size": 10, 
             "query": {"bool": {"must": [
                             { "exists": {"field": "profile.report"}},
                             {"multi_match" : {"query": kwd, 
                            "fields": ["metadata.title", "metadata.keywords", "metadata.description"]}}]
     }}}
    
    with open('../settings.json') as f:
        j = load(f)
        
    client = Elasticsearch(j["ElasticSearch"]['es_url'])
    response = client.search(index=j["ElasticSearch"]['es_index'], body=query)
    
    d = ((hit['_source']['metadata']['id'], hit['_source']['title']) for hit in response['hits']['hits'] )
    d = list(islice(d, 10))
    return d

def fetch_ids_by_title(title):
    if title is None:
        return []
    
    
    query = { "track_scores": "true", "_source": ["_id", "metadata.title"],
             "query": { "bool": { "must": [{ "match": {"metadata.title": title}}],
                                 "filter": [ {"exists": {"field": "profile.report"}}]
                                 }}}
    
    with open('../settings.json') as f:
        j = load(f)
        
    client = Elasticsearch(j["ElasticSearch"]['es_url'])
    response = client.search(index=j["ElasticSearch"]['es_index'], body=query)
    
    print(query)
    print()
    out = []
    for i, hit in enumerate(response['hits']['hits']):
        if i< 2:
            print(hit)
        out += [{'_id': hit['_id'], 'title': hit['_source']['metadata']['title']}]
    print()
    print(out[:2])
    return out

def fetch_id(rid=None):
    if  rid is None:
        return None
    else:
        query = {"track_scores":"true",
                 "query": { "match": {"_id": rid} }, "size": 1}
    
    with open('../settings.json') as f:
        j = load(f)
    
    print(rid)
    client = Elasticsearch(j["ElasticSearch"]['es_url'])
    response = client.search(index=j["ElasticSearch"]['es_index'], body=query)
    
    for hit in response['hits']['hits']:
        break
    
    return hit


def switch_item(d, key, val=-1):
    d2 = d
    if key == 'min':
        key = '0%'
    if key == 'max':
        key = '100%'
    d2['key'] = key
    if val != -1:
        d2['value'] = val
    return d2

def search_field(d, key):
    for val in d:
        if val['key'] == key:
            return val['value']
        

def transform_general(d):
    d2 = {}
    d2['Number of variables'] = d['n_var']
    d2['Number of observations'] = d['n']
    d2['Missing cells'] = d['n_cells_missing']
    d2['Missing cells (%)'] = '{:.2f}%'.format(100*d['p_cells_missing'])
    d2['Duplicate rows'] = d['n_duplicates']
    d2['Duplicate rows (%)'] = '{:.2f}%'.format(100*d['p_duplicates'])
    d2['Total size in memory'] = '{}.0B'.format(d['memory_size'])
    d2['Average record size in memory'] = '{:.2f}.0B'.format(d['record_size'])
    return d2

def transform_variable(d, dtype):
    d1, d2 = [], []

    for item in d:
        if item['key'] == 'n_distinct':
            d1.append(switch_item(item, 'Distinct'))
        elif item['key'] == 'p_distinct':
            d1.append(switch_item(item, 'Distinct (%)', '{:.2f}%'.format(100*float(item['value']))))
        elif item['key'] == 'n_missing':
            d1.append(switch_item(item, 'Missing'))
        elif item['key'] == 'p_missing':
            d1.append(switch_item(item, 'Missing (%)', '{:.2f}%'.format(100*float(item['value']))))
        elif item['key'] == 'memory_size':
            d1.append(switch_item(item, 'Memory', '{}.0B'.format(item['value'])))

        if dtype in ['Numeric', 'Temporal']:
            if item['key'] in ['min', '5%', '25%', '50%', '75%', '95%', 'max']:
                d2.append(switch_item(item, item['key']))
        elif dtype == 'Spatial':
            if item['key'] in ['x_min', 'y_min', 'x_max', 'y_max']:
                d2.append(switch_item(item, item['key']))
    return d1, d2

def retrieve_plot(var):
    fig = ""
    if 'freqs' in var:
        df = pd.DataFrame(var['freqs'])
        df.columns = ['Frequency', 'Word']
        df = df.sort_values('Frequency', ascending=False)
        fig = px.bar(df, x="Frequency", y="Word", orientation='h',
                     category_orders={'Word': df['Word'].values.tolist()})
        fig = dcc.Graph(figure=fig, style= {'width': '500px', 'height': '300px'})
    elif var['type'] in ['Numeric', 'Temporal']:
        df = pd.DataFrame(var['stats'][1])
        df.columns = ['Value', 'Percentage']
        if var['type'] == 'Numeric':
            df.Value = df.Value.astype(float)
        else:
            df.Value = df.Value.apply(parse)
        df.Percentage = pd.Categorical(df.Percentage, categories=['0%','5%','25%','50%','75%','95%','100%'], ordered=True)
        df = df.sort_values('Percentage', ascending=True)
        fig = px.bar(df, x="Percentage", y="Value")
        fig = dcc.Graph(figure=fig, style= {'width': '500px', 'height': '300px'})
    elif var['type'] == 'Spatial':
        df = pd.DataFrame(var['stats'][1]).set_index('key')
        df.value = df.value.astype(float)
        #minx, miny, maxx, maxy = df.value.loc[['x_min', 'y_min', 'x_max', 'y_max']].values.tolist()
        minx, miny, maxx, maxy = df.value.loc[['y_min', 'x_min', 'y_max', 'x_max']].values.tolist()
        bb = box(minx, miny, maxx, maxy)
        map_center = [bb.centroid.y, bb.centroid.x]
        m = folium.Map(location=map_center, tiles='OpenStreetMap', width='100%', height='100%',
                       zoom_control=False, scrollWheelZoom=False, dragging=False)
        #m.fit_bounds(([bb.bounds[3], bb.bounds[2]], [bb.bounds[1], bb.bounds[0]]))
        m.fit_bounds(([bb.bounds[1], bb.bounds[0]], [bb.bounds[3], bb.bounds[2]]))
        fig = html.Iframe(srcDoc=m.get_root().render(), width='300px', height='150px')
             
    return fig

def transform_dataset(X):
    variables = X['_source']['profile']['report']['variables']
    X['_source']['profile']['report']['summary'] = transform_general(X['_source']['profile']['report']['table'])
    for i in range(len(variables)):
        variables[i]['stats'] = transform_variable(variables[i]['stats'], variables[i]['type'])
        variables[i]['fig'] = retrieve_plot(variables[i])
    del X['_source']['profile']['columns']
    return X


def retrieve_column(X, col):
    for no, var in enumerate(X['_source']['profile']['report']['variables']):
        if var['name'] == col:
            break
        
    cols=[{"name": i, "id": i, "selectable": True} for i in ["key", "value"]]
    
    sel_title_div = html.Div(children = [
                        html.H2(children='Selected Attribute Preview', style={'color': '#7100FF'}),
                    ], style={'margin-right': '5%'})
    
    title_div = html.Div(children = [
                        html.H3(children=col, style={'color': '#7100FF'}),
                        html.P(children=var['type']),
                    ], style={'margin-right': '5%'})
    
    info_children = [dash_table.DataTable(columns=cols,
                                       data=var['stats'][0], style_as_list_view=True,
                                       style_header={'display':'none'},
                                       style_table={'whiteSpace': 'normal', 'height': 'auto', 
                                                    'width': '40%', 'margin-left':'20px'},)]
    #info_children += [retrieve_plot(var)]
    info_children += [var['fig']]
    
    info_div = html.Div(children=info_children, style={'display':'block', 'margin-left':'20px'})

    content = html.Div(children=[sel_title_div, title_div, info_div], 
                       style={'display':'block', 'margin-left':'20px', 'margin-top':'10%'})
        
    return content

def retrieve_metadata(X, field):
    sel_title_div = html.Div(children = [
                        html.H2(children='Selected Attribute Preview', style={'color': '#7100FF'}),
                    ], style={'margin-right': '5%'})
    
    title_div = html.Div(children = [
                        html.H3(children=field, style={'color': '#7100FF'}),
                        #html.P(children=var['type']),
                    ], style={'margin-right': '5%'})
    
    info_children = ""
    if field in X:
        info_children = html.Div(children = [
                            html.P(children=X[field]),
                            ], style={'margin-right': '5%'})
    
    info_div = html.Div(children=info_children, style={'display':'block', 'margin-left':'20px'})

    content = html.Div(children=[sel_title_div, title_div, info_div], 
                       style={'display':'block', 'margin-left':'20px', 'margin-top':'10%'})
        
    return content

def retrieve_info(X, field, content):
    if content:
        col = retrieve_column(X, field)
    else:
        col = retrieve_metadata(X['_source']['metadata'], field)
    return col

def retrieve_general(X):
    gf = X['_source']['profile']['report']['summary']
    df_2 = [{'Dataset statistics': k, '': v} for k, v in gf.items()]
    cols_2=[{"name": i, "id": i, "selectable": True} for i in ["Dataset statistics", ""]]
    
    df_3 = [{'Variable types': k, '': v} for k, v in X['_source']['profile']['report']['table']['types'].items()]
    cols_3=[{"name": i, "id": i, "selectable": True} for i in ["Variable types", ""]]
    
    children = html.Div(children=[
            dash_table.DataTable(
                id='var_general_2',
                columns=cols_2,
                data=df_2,
                style_as_list_view=True,
                style_header={'font-size':'1.5em'},
                style_table={'whiteSpace': 'normal', 'height': 'auto',
                             'width': '10%','margin-left':'20px'
                },),
            dash_table.DataTable(
                id='var_general_3',
                columns=cols_3,
                data=df_3,
                style_as_list_view=True,
                style_header={'font-size':'1.5em'},
                style_table={ 'whiteSpace': 'normal', 'height': 'auto',
                             'width': '10%', 'margin-left':'20px',
                             'margin-top':'20px'
                },)
            ], style={'display': 'block', 'margin-top': '5%'})
    
    return children

def make_item(i, name, prefix, body):
    # we use this function to make the example items to avoid code duplication
    
    card = dbc.Card(
        [
            dbc.CardHeader(
                html.H4(
                    html.Span(
                        f"{name}",
                        id=f"{prefix}_toggle_{i}",
                    )
                ),
                style= {'padding':'0'}
            ),
            dbc.Collapse(
                dbc.CardBody(body, style={'overflow': 'auto'} ),
                id=f"{prefix}_collapse_{i}",
            ),
        ]
    )
    
    return card

def make_item_table(prefix, i):
    return dash_table.DataTable(
            id=f'{prefix}_data_{i}',
            columns=[{"name": "", "id": "col_id", "selectable": True, "type":'text'}],
            data=[],
            row_selectable="multi",
            style_as_list_view=True,
            style_header={'font-size':'1.0em'},
            style_table={
                'whiteSpace': 'normal',
                'height': 'auto',
                },
            style_cell={
                'whiteSpace': 'normal',
                'height': 'auto',
                },  
            ),
                    

def make_items(prefix):
    types = ['Content', 'Categorical', 'Numeric', 'Temporal', 'Spatial', 'Metadata']
    #return [make_item(i, name, prefix) for i, name in enumerate(types)]
    #div1 = [make_item(i, name, prefix) for i, name in enumerate(types[:5])]
    
    content = [make_item(1, 'Categorical', prefix, make_item_table(prefix, 1)),
               make_item(2, 'Numeric', prefix, make_item_table(prefix, 2)),
               make_item(3, 'Temporal', prefix, make_item_table(prefix, 3)),
               make_item(4, 'Spatial', prefix, make_item_table(prefix, 4))]
    
    div1 = [make_item(0, 'Content', prefix, content)]
    
    div2 = [make_item(5, 'Metadata', prefix, make_item_table(prefix, 5))]
    
    
    return html.Div(children=[
        html.Div(div1, className="accordion"),
        html.Div(div2, className="accordion"),
        ])

    

def retrieve_names(X):
    out = {'Categorical':[], 'Numeric':[], 'Temporal':[], 'Spatial':[], 'Metadata':[]}
    for no, val in enumerate(X['_source']['profile']['report']['variables']):
        if val['type'] in out:  # No unsupported
            out[val['type']].append({'col_id': val['name']})
        
    if 'metadata' in X['_source']:
        for field in ['title', 'keywords', 'description']:
            if field in X['_source']['metadata']:
                out['Metadata'].append({'col_id': field})
    return out


def find_var_in_lists(var, names):
    for i, name in enumerate(names):
        try:
            pos = name.index({'col_id': var})
            return (i, pos)
        except:
            continue
