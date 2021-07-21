# -*- coding: utf-8 -*-

# Run this app with `python app.py` and
# visit http://127.0.0.1:8050/ in your web browser.

from os import sep
from re import LOCALE
import re
import dash
from dash import dependencies
import dash_core_components as dcc
import dash_html_components as html
from pandas.core.accessor import PandasDelegate
import plotly.express as px
import pandas as pd
from datetime import datetime
#--------------
import dash_table
import dash_table.FormatTemplate as FormatTemplate
from dash_table.Format import Sign
from collections import OrderedDict

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

# assume you have a "long-form" data frame
# see https://plotly.com/python/px-arguments/ for more options
df = pd.read_csv('input/panel/210713-nc.csv', sep=';', dtype='object')

df['F_COMPRA'] = pd.to_datetime(df['F_COMPRA'])
df['F_COMPRA_ANT'] = pd.to_datetime(df['F_COMPRA_ANT'])

df.loc[:,'CANT'] = -pd.to_numeric(df['CANT'])
df.loc[:,'TOTAL_COSTO'] = pd.to_numeric(df['TOTAL_COSTO'])

df['MES_NC'] = df['F_COMPRA'].dt.strftime('%b')
df['MES_COMPRA'] = df['F_COMPRA_ANT'].dt.strftime('%b')

dftiendas1 = df[(df['NLOCAL_CREACION']!='FALABELLA. COM ALTERNO')&(df['NLOCAL_CREACION']!='ADMINISTRATIVO')&(df['NLOCAL_CREACION']!='VENTA EMPRESA')]
dftiendas = dftiendas1.groupby(['NLOCAL_CREACION','MES_NC']).agg({'CANT':'sum','TOTAL_COSTO':'sum','CAUTORIZA':'nunique'}).reset_index()

meses = sorted(dftiendas['MES_NC'].unique(), key=lambda m: datetime.strptime(m, "%b"))
fig = px.scatter(dftiendas, y="TOTAL_COSTO", x="CAUTORIZA", hover_data=['NLOCAL_CREACION'])

app.layout = html.Div(children=[
    html.H1(children='Falabella'),

    html.Div(children='''
        Reporte Notas Crédito
    '''),

    html.Div([  
    
    html.Div([
        dcc.Graph(
            id='example-graph',
            hoverData={'points': [{'customdata': 'COLINA'}]}
        )
    ], style={'width': '49%'}),

   html.Div(dcc.Slider(
        id='crossfilter-month--slider',
        min=0,
        max= len(meses)-1,
        value= len(meses)-1,
        marks={str(value): str(month) for value,month  in enumerate(meses)},
        step=None
    ), style={'width': '49%', 'padding': '0px 20px 20px 20px'}),

    html.Div(
        dash_table.DataTable(
            id='tabla',
            columns=[{"name": i, "id": i} for i in dftiendas.columns],
            page_current=0,
            page_size=10,
            page_action='custom',
            #filter_action='custom',
            #filter_query=''
        ),
        style={'width': '49%'}),

    ]),
])

@app.callback(
    dash.dependencies.Output('example-graph', 'figure'),
    [dash.dependencies.Input('crossfilter-month--slider', 'value')]
)
def update_graph(month):
    dfauxmonth = dftiendas[dftiendas['MES_NC']==meses[month]]
    return px.scatter(dfauxmonth, y="TOTAL_COSTO", x="CAUTORIZA", color='NLOCAL_CREACION', size='CANT', hover_data=['NLOCAL_CREACION'])


@app.callback(
    dash.dependencies.Output('tabla', 'data'),
    dash.dependencies.Input('example-graph', 'hoverData'),
    dash.dependencies.Input('tabla', "page_current"),
    dash.dependencies.Input('tabla', "page_size"))

def update_table(local, page_current, page_size):
    var = local['points'][0]['customdata'][0]
    print(var)
    dflocal = dftiendas
    dflocal = dflocal.loc[dflocal['NLOCAL_CREACION']==var]
    print(page_current)
    print(page_size)
    return dflocal.iloc[
    page_current*page_size:(page_current+ 1)*page_size
    ].to_dict('records')

if __name__ == '__main__':
    app.run_server(debug=True)