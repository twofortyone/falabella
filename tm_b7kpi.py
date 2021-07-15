# Librerías
import pandas as pd
from datetime import datetime
from cl_cleaning import CleaningText as ct 
from ica_raw import InternalControlAnalysis

# Configuraciones
pd.set_option('float_format', '{:,.2f}'.format)

# Variables 
dt_string = datetime.now().strftime('%y%m%d-%H%M%S')
index_name = 'indice_b7'
cost_column = 'ct'

# Importar bases
kpi = pd.read_csv('input/210528-153310-kpi.csv', sep=';', dtype='object')
b7 = pd.read_csv('input/base7.csv', sep=';', dtype='object')

# Normalizar nombres de columnsa 
kpi = ct.normalizar_cols(kpi)
b7 = ct.normalizar_cols(b7)

# Convertir columnas de precio a dato numérico
b7 = ct.convertir_a_numero(b7, ['costo rep','ct'])

# Generar indice en columna
b7.reset_index(inplace=True)
b7.rename(columns={'index': index_name}, inplace=True)

#kpi['aaaa paletiza'] = kpi['fecha_paletiza'].str.extract('(\d{4})') ya lo trae el archivo 

# Análisis de KPI
ica = InternalControlAnalysis(b7, index_name, cost_column)

dfccd = b7[b7['tipificacion']=='CERRADO - Recibido en CD ']
nb7ccd = dfccd.shape[0]
cb7ccd= dfccd[[cost_column]].sum()

dfccd2, nfnan, cfnan = ica.get_fnan( dfccd, 'f12', 'KPI')
dfccd3, ndu, cdu = ica.get_duplicates( dfccd2, ['f12','ean','qcanpedu'], 'KPI')
ne, nne, cne = ica.get_notfound( dfccd3, kpi, ['f12'], ['entrada'], 'entrada', 'KPI')
dfkpi = pd.merge(dfccd3, kpi, left_on=['f12'], right_on=['entrada']) # Unir b2345 con KPI
dfkpi2, ndy, cdy = ica.get_diffyear(dfkpi, 'aaaa paletiza', '2021', 'KPI')

iokkpi = dfkpi2[index_name].values
b7 = ica.get_db()
b7.loc[iokkpi, 'CIKPI'] = 'OKK'
b7.loc[iokkpi, 'CIA'] = 'OKK'
# Reporte 
print('\n ----------------- Base 7 ----------------- ')
print('\n ## Resumen de información según tipificacion')
print(b7[['tipificacion', cost_column]].groupby('tipificacion').sum().sort_values(by=cost_column, ascending=False))
nan = [nfnan, nb7ccd, cfnan]
du = [ndu, dfccd2.shape[0], cdu]
nex = [nne, dfccd3.shape[0], cne]
# ncc = [nncc, b6f53.shape[0], cncc]
daa = [ndy, dfkpi.shape[0], cdy]
total = [nfnan+ndu+nne+ndy, nb7ccd,cfnan+cdu+cne+cdy ]
summaryres = b7[[cost_column, 'CIKPI', 'tipificacion']].groupby(['tipificacion', 'CIKPI']).agg(['sum', 'size']).sort_values(by=(cost_column,'sum'), ascending=False)
# reporte.print_analysis(comp='F5', comments='para estado cerrado', nan=nan, du=du, ne=nex, dc=ncc, summary=summaryres)
#reporte.print_analysis(comp='KPI', comments='para tipificación CERRADO - Recibido en CD', total=total, nan=nan, du=du, ne=nex, daa=daa, summary=summaryres)

# Tareas finales 
b7.to_csv(f'output/{dt_string}-b7.csv', sep=';', decimal=',', index=False) # Guarda el archivo 
b7mkpi = b7.merge(kpi, how='left', left_on=['f12'], right_on=['entrada'])
b7mkpi.drop_duplicates(subset=['indice_b7'], inplace=True)
b7mkpi.to_csv(f'output/{dt_string}-b7mkpi.csv', sep=';', decimal=',', index=False)