import pandas as pd
pd.options.plotting.backend = "plotly"
from cl_cleaning import CleaningText as ct 
import plotly.express as px
import locale 

locale.setlocale(locale.LC_ALL, 'es_ES.UTF-8')


f11 = pd.read_csv(f'input/f11.csv', sep=';', dtype='object')

f11.columns
f11 = ct.convertir_a_numero(f11, ['TOTAL_COSTO_PROMEDIO'])
gb = f11.groupby(['PROPIETARIO','FECHA CORTE'])['TOTAL_COSTO_PROMEDIO'].sum()
df = gb.reset_index()
fig = px.line(df, x='FECHA CORTE', y='TOTAL_COSTO_PROMEDIO', color='PROPIETARIO')
fig.show()

df2021 = f11[f11['AÑO']=='2021']
gb = df2021.groupby(['PROPIETARIO','FECHA CORTE'])['TOTAL_COSTO_PROMEDIO'].sum()
df = gb.reset_index()
fig = px.line(df, x='FECHA CORTE', y='TOTAL_COSTO_PROMEDIO', color='PROPIETARIO')
fig.show()

f11.TOTAL_COSTO_PROMEDIO.sum()

f11['FECHA CORTE'] = f11['FECHA CORTE'] +' '
f11['FECHA'] = f11['FECHA CORTE'] + f11['AÑO']
f11.FECHA = f11.FECHA.str.replace('  ', ' ')
f11.FECHA = f11.FECHA.str.replace(' ', '-')
f11.FECHA = f11.FECHA.str.replace('DIC', 'Diciembre')
f11['fpandas'] = pd.to_datetime(f11.FECHA, format='%d-%B-%Y')
gb = f11.groupby(['PROPIETARIO','fpandas'])['TOTAL_COSTO_PROMEDIO'].sum()
df = gb.reset_index()
fig = px.line(df, x='fpandas', y='TOTAL_COSTO_PROMEDIO', color='PROPIETARIO')
fig.show()
