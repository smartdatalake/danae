from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search, Q
from elasticsearch.helpers import scan
from creds import hosts, es_index
from itertools import islice
import dash_core_components as dcc
import dash_html_components as html
import dash_table
import pandas as pd
import plotly.express as px
from styles import style_sdl, style_sdl_light
from collections import Counter
            
types = {'cat': set(['Variable.TYPE_CAT', 'Categorical']),
         'num': set(['Variable.TYPE_NUM', 'Numeric']), 
         'date': set(['Variable.TYPE_DATE', 'DateTime'])}

def fetch_ids(kwd):
    if kwd is None or kwd == '':
        return None

    client = Elasticsearch(hosts=hosts)
    
    kwd = kwd.lower()
    
    query = {"track_scores":"true", "_source": ["title","metadata.id"], "size": 10, 
             "query": {"bool": {"must": [
                             { "exists": {"field": "profile.report"}},
                             {"multi_match" : {"query": kwd, 
                            "fields": ["metadata.title", "metadata.keywords", "metadata.description"]}}]
     }}}
    
    response = client.search(index=es_index, body=query)
    
    d = ((hit['_source']['metadata']['id'], hit['_source']['title']) for hit in response['hits']['hits'] )
    d = list(islice(d, 10))
    return d


def fetch_id(d_title=None, d_id=None):
    print(d_title, d_id)
    if d_title is None and d_id is None:
        return None
    else:
        if d_title is not None:
            query = {"track_scores":"true",
                     "query": { "match": {"metadata.title": d_title} }, "size": 1}
        elif d_id is not None:
            query = {"track_scores":"true",
                     "query": { "match": {"metadata.id": d_id} }, "size": 1}
    
    client = Elasticsearch(hosts=hosts)
    
    response = client.search(index=es_index, body=query)
    for hit in response['hits']['hits']:
        break
    
    return hit

def make_general_tab(X):
    gf = transform_general_data(X)
    df_2 = [{'Dataset statistics': k, '': v} for k, v in gf.items()]
    cols_2=[{"name": i, "id": i, "selectable": True} for i in ["Dataset statistics", ""]]
    
    df_3 = [{'Variable types': k, '': v} for k, v in X['types'].items()]
    cols_3=[{"name": i, "id": i, "selectable": True} for i in ["Variable types", ""]]
    
    children = [dcc.Tab(label='General', value='tab_general', children=[
        html.Div(children=[
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
                             'width': '10%', 'margin-left':'20px'
                },)
            ], style={'display': 'flex', 'margin-top': '5%'}),      
        ], style=dict(style_sdl, **{'width':'auto', 'display': 'block'}),)]
    
    return children


def make_variable_tab(val, id, no):
    key = val['name']    
    
    cols=[{"name": i, "id": i, "selectable": True} for i in ["key", "value"]]
    key = val['name']    
    
    v_type, v_unique, df = transform_data(val['stats'])
    df1 = None
    
    if len(df)>6:
        df1 = df[len(df)//2:] 
        df = df[:len(df)//2] 

    title_div = html.Div(id='var_general_data_'+key, children = [
                    html.H3(id='var_title_'+key, children=key, style={'color': '#7100FF'}),
                    html.P(id='var_type_'+key, children=v_type),
                    html.Code(id='var_unique_'+key, children=v_unique, style=style_sdl_light),
                    ], style={'margin-right': '5%'})
    
    info_children = [dash_table.DataTable( id='var_data_'+key, columns=cols,
                                       data=df, style_as_list_view=True,
                                       style_header={'display':'none'},
                                       style_table={'whiteSpace': 'normal', 'height': 'auto', 
                                                    'width': '40%', 'margin-left':'20px'},)]

    if df1 is not None:
         info_children +=  [dash_table.DataTable(
                        id='var_data_{}_2'.format(key), columns=cols,
                        data=df1, style_as_list_view=True,
                        style_header={'display':'none'},
                        style_table={'whiteSpace': 'normal', 'height': 'auto',
                                     'width': '40%', 'margin-left':'20px'},)]
         
    v_type = search_field(val['stats'], 'type')
    p_missing = float(search_field(val['stats'], 'Missing (%)')[:-1])
    if v_type in types['cat'] and p_missing < 10.0:
        df = fetch_top_k(id, no, k=10)
        
        df = pd.DataFrame(df)
        df.columns = ['Word', 'Frequency']
        df = df.sort_values('Frequency', ascending=False)
        fig = px.bar(df, x="Frequency", y="Word", orientation='h',
                     category_orders={'Word': df['Word'].values.tolist()})
        fig = dcc.Graph(figure=fig, style= {'width': '500px', 'height': '300px'})
        info_children.append(fig)
    
    info_div = html.Div(children=info_children, style={'display':'flex', 'margin-left':'20px'})

    content = html.Div(children=html.Div(children=[title_div, info_div], 
                                          style={'display':'block', 'margin-left':'20px'}),
                        style={'display': 'block'})
    
    return [dcc.Tab(label=key, value='tab_'+key, children=content, 
                    className='varClass', style=dict(style_sdl, **{'width':'auto'})),]
    
def transform_data(d):
    d2 = []
    ctype, is_unique = None, None

    for item in d:
        if item['key'] == 'n_distinct':
            d2.append(switch_item(item, 'Distinct'))
        elif item['key'] == 'p_distinct':
            d2.append(switch_item(item, 'Distinct (%)', '{:.2f}%'.format(100*float(item['value']))))
        elif item['key'] == 'n_missing':
            d2.append(switch_item(item, 'Missing'))
        elif item['key'] == 'p_missing':
            d2.append(switch_item(item, 'Missing (%)', '{:.2f}%'.format(100*float(item['value']))))
        elif item['key'] == 'is_unique':
            is_unique = 'UNIQUE' if item['value'] == 'True' else ''
        elif item['key'] == 'type':
            if item['value'] in types['cat']:
                ctype = 'Categorical'
            elif item['value'] in types['num']:
                ctype = 'Real number (ℝ≥0)'
            elif item['value'] in types['date']:
                ctype = 'DateTime'
        elif item['key'] == 'n_infinite':        
            d2.append(switch_item(item, 'Infinite'))
        elif item['key'] == 'p_infinite':
            d2.append(switch_item(item, 'Infinite (%)', '{:.2f}%'.format(100*float(item['value']))))            
        elif item['key'] == 'mean':
            d2.append(switch_item(item, 'Mean', '{:.4f}'.format(float(item['value']))))
        elif item['key'] == 'min':        
            d2.append(switch_item(item, 'Minimum'))
        elif item['key'] == 'max':        
            d2.append(switch_item(item, 'Maximum'))
        elif item['key'] == 'range':        
            d2.append(switch_item(item, 'Range'))            
        elif item['key'] == 'n_zeros':        
            d2.append(switch_item(item, 'Zeros'))
        elif item['key'] == 'p_zeros':
            d2.append(switch_item(item, 'Zeros (%)', '{:.2f}%'.format(100*float(item['value']))))            
        elif item['key'] == 'memory_size':
            d2.append(switch_item(item, 'Memory', '{}.0B'.format(item['value'])))
            
    return ctype, is_unique, d2

def transform_general_data(d):
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

def switch_item(d, key, val=-1):
    d2 = d
    d2['key'] = key
    if val != -1:
        d2['value'] = val
    return d2

def search_field(d, key):
    for val in d:
        if val['key'] == key:
            return val['value']
        
def fetch_top_k(id, col, k=10):
    client = Elasticsearch(hosts=hosts)
    field = 'profile.columns.{}'.format(col)
    r = client.termvectors('danae-eodp', id=id, fields=[field], offsets=False, positions=False)
    freqs = {k: v['term_freq'] for k,v in r['term_vectors'][field]['terms'].items()}
    c = Counter(freqs)
    return c.most_common(k)        


