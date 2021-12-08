import dash
import dash_core_components as dcc
import dash_html_components as html
from styles import style_div, style_drop_list
from dash.dependencies import Output
import dash_table
from dash_table.Format import Format, Scheme
import dash_bootstrap_components as dbc
#from dash import html
from methods import make_items


result_columns = [
                  #{"name": "Query ID", "id": "query_id", "selectable": True, "type":'text'},
                  {"name": "Dataset ID", "id": "_id", "selectable": True, "type":'text'},
                  {"name": "Dataset Title", "id": "result_title", "selectable": True, "type":'text'},
                  {"name": "Overall Score", "id": "overall_score", "type":'numeric', "format": Format(precision=2, scheme=Scheme.fixed)},
                  {"name": "Content Score", "id": "content_score", "type":'numeric', "format": Format(precision=2, scheme=Scheme.fixed)},
                  {"name": "Metadata Score", "id": "metadata_score", "type":'numeric', "format": Format(precision=2, scheme=Scheme.fixed)}
                  ]
                  
                  
query_columns = [
                  {"name": "Dataset ID", "id": "_id", "selectable": True, "type":'text'},
                  {"name": "Dataset Title", "id": "title", "selectable": True, "type":'text'},
                  
                  ]



query_tab_1 = dbc.Card(
    dbc.CardBody(
        [
            html.Div(dcc.Loading(html.Div(
                 children=[dash_table.DataTable(
                         id='query_list',
                         columns=query_columns,
                         data=[],
                         page_size=5,
                         row_selectable="single",
                         style_as_list_view=True,
                         style_header={'font-size':'1.0em'},
                         style_table={
                             'whiteSpace': 'normal',
                             'height': 'auto',
                             },
                         # style_cell={
                         #     'whiteSpace': 'normal',
                         #     'height': 'auto',
                         #     #'overflow': 'hidden',
                         #     #'textOverflow': 'ellipsis',
                         #     #'maxWidth': 0,
                         #     },
                        style_cell_conditional=[
                            {'if': {'column_id': '_id'},
                              # 'overflow': 'hidden',
                              # 'textOverflow': 'ellipsis',
                              # 'maxWidth': 0,},
                              },
                            {'if': {'column_id': 'title'},
                             'whiteSpace': 'normal',
                             'height': 'auto'},
                        ]                         
                         ),
                     ],
                 ))),
        ]
    ),
    className="mt-3",
)

result_tab_1 = dbc.Card(
    dbc.CardBody(
        [
            html.Div(dcc.Loading(html.Div(id='results',
                 children=[dash_table.DataTable(
                         id='results_list',
                         columns=result_columns,
                         data=[],
                         page_size=5,
                         row_selectable="single",
                         style_as_list_view=True,
                         style_header={'font-size':'1.0em'},
                         style_table={
                             'whiteSpace': 'normal',
                             'height': 'auto',
                             },
                         style_cell_conditional=[
                            {'if': {'column_id': '_id'},
                              # 'overflow': 'hidden',
                              # 'textOverflow': 'ellipsis',
                              # 'maxWidth': 0,},
                              },
                            {'if': {'column_id': 'result_title'},
                             'whiteSpace': 'normal',
                             'height': 'auto'},
                        ]         
                         ),
                     ],
                 )))
        ]
    ),
    className="mt-3",
)

query_tab_2 = dbc.Card(
    dbc.CardBody(
        [
            html.Div(id='query_general'),
        ]
    ),
    className="mt-3",
)

result_tab_2 = dbc.Card(
    dbc.CardBody(
        [
            html.Div(id='result_general'),
        ]
    ),
    className="mt-3",
)

query_tab_3 = dbc.Card(
    dbc.CardBody(
        [
            html.Div(children=[
                    html.Div(children = [
                        html.H2(children='Available Attributes', style={'color': '#7100FF'}),
                        ], style={'margin-right': '5%'}),
                    html.Div(children=[
                            html.Button('Select All', id='btn_select'),
                            html.Button('Unselect All', id='btn_unselect'),
                            ]),
                    # html.Div(make_items('query'), className="accordion")
                    make_items('query'),
                    ]),
            html.Hr(),
            dbc.Card( [
                dbc.CardHeader(
                    html.H4(
                        html.Span('Selected Attribute Preview',
                        id='query_toggle_preview', style={'color': '#7100FF'}
                    ))),
                dbc.Collapse(
                    dbc.CardBody(html.Div(id='query_field')),
                    id= "query_collapse_preview",
                    ),
                ])
        ]
    ),
    className="mt-3",
)

result_tab_3 = dbc.Card(
    dbc.CardBody(
        [
            html.Div(children=[
                html.Div(children = [
                        html.H2(children='Available Attributes', style={'color': '#7100FF'}),
                        ], style={'margin-right': '5%'}),
                #html.Div(make_items('result'), className="accordion")
                make_items('result'),
                ]),
            html.Hr(),
            dbc.Card( [
                dbc.CardHeader(
                    html.H4(
                        html.Span('Selected Attribute Preview',
                        id='result_toggle_preview', style={'color': '#7100FF'}
                    ))),
                dbc.Collapse(
                    dbc.CardBody(html.Div(id='result_field')),
                    id= "result_collapse_preview",
                    ),
                ])
        ]
    ),
    className="mt-3",
)


query_tab = dbc.Card(
    dbc.CardBody(
        [
            html.P("This is tab 2!", className="card-text"),
            dbc.Button("Don't click here", color="danger"),
        ]
    ),
    className="mt-3",
)

query_tabs = html.Div(children=[
     html.H3("Query Dataset"),
     dbc.Tabs(
        [
            dbc.Tab(query_tab_1, label="Dataset Selection"),
            dbc.Tab(query_tab_2, label="Dataset Profiling"),
            dbc.Tab(query_tab_3, label="Attributes"),
            #dbc.Tab(query_tab, label="Variable Synopsis", disabled=True),
        ]
    )
 ], style={'width': '40%'})

result_tabs = html.Div(children=[
     html.H3("Result Dataset"),
     dbc.Tabs(
        [
            dbc.Tab(result_tab_1, label="Dataset Selection"),
            dbc.Tab(result_tab_2, label="Dataset Profiling"),
            dbc.Tab(result_tab_3, label="Attributes"),
        ]
    )
 ], style={'width': '40%', 'margin-left': '10%'})

tabs = html.Div(children=[query_tabs, result_tabs], style={'display':'flex'})