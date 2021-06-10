# Librerías
from os import sep
import pandas as pd
from datetime import datetime
from cleaning import CleaningText as ct 

# Variables
now = datetime.now()
dt_string = now.strftime("%y%m%d-%H%M%S")

kpi = pd.read_excel('input/210608_kpi.xlsx', dtype='object')
kpi = ct.normalizar_cols(kpi)
kpi.rename(columns={'index': 'ind'}, inplace=True)

kpi.reset_index(inplace=True)

kpi['aaaa paletiza'] = kpi['fecha_paletiza'].str.extract('(\d{4})')
kpi['entrada'] = kpi.entrada.str.extract('(\d+)', expand=False)

du = kpi[kpi.duplicated(subset=['entrada'],keep=False)]

td = du[du['aaaa paletiza'] !='2021']

kpi.drop(index=td['index'].values, inplace=True)

kpi.drop_duplicates(subset=['entrada'], inplace=True) # Agradado el 1 de junio para correcci{on }

du.to_csv(f'output/{dt_string}-du.csv', sep=';', decimal=',', index=False)
kpi.to_csv(f'output/{dt_string}-kpi.csv', sep=';', decimal=',', index=False)