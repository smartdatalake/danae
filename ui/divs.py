#!/usr/bin/env python3
import dash
import dash_core_components as dcc
import dash_html_components as html
from styles import style_div, style_drop_list
from dash.dependencies import Output
import dash_table
from dash_table.Format import Format, Scheme

result_columns = [
                  {"name": "Dataset ID", "id": "result_id", "selectable": True, "type":'text'},
                  {"name": "Dataset Title", "id": "result_title", "selectable": True, "type":'text'},
                  {"name": "Overall Score", "id": "overall_score", "type":'numeric', "format": Format(precision=2, scheme=Scheme.fixed)},
                  {"name": "Content Score", "id": "content_score", "type":'numeric', "format": Format(precision=2, scheme=Scheme.fixed)},
                  {"name": "Metadata Score", "id": "metadata_score", "type":'numeric', "format": Format(precision=2, scheme=Scheme.fixed)}
                  ]
                  
                  
