# Librerías
import pandas as pd
from datetime import datetime
from cl_cleaning import CleaningText as ct 
from ica_raw import InternalControlAnalysis

# Configuraciones
pd.set_option('float_format', '{:,.2f}'.format)

# Variables 
dt_string = datetime.now().strftime('%y%m%d-%H%M')
index_name = 'indice_b6'
cost_column = 'costo total'

# Importar F5 enviado, reservado y recibido
f5e = pd.read_csv('input/f5_enviado.csv', sep=';', dtype='object')
f5rec = pd.read_csv('input/f5_reservado.csv', sep=';', dtype='object')
f5res = pd.read_csv('input/f5_recibido.csv', sep=';', dtype='object')
b6 = pd.read_csv('input/base6.csv', sep=';', dtype='object')

lf5 = [f5e, f5rec, f5res]
f5 = pd.concat(lf5, axis=0)

# Normalizar nombres de columnsa 
f5 = ct.normalizar_cols(f5)
b6 = ct.normalizar_cols(b6)
b6 = ct.col_duplicados(b6, 'estado')

# Convertir columnas de precio a dato numérico
b6 = ct.convertir_a_numero(b6, ['costo', 'costo total'])

# Generar indice en columna
b6.reset_index(inplace=True)
b6.rename(columns={'index': index_name}, inplace=True)

# Obtener el año de la reserva, el envío y la recepción
colsf5 = ['fe. reserva', 'fe. envio', 'fe. recep']
newcolsf5 = ['aaaa reserva', 'aaaa envio', 'aaaa recep']
f5[newcolsf5] = f5[colsf5].apply(lambda x: x.str.extract('(\d{4})', expand=False))

colsb6 = ['fecha compra', 'fecha cambio-devolucion']
newcolsb6 = ['aaaa compra', 'aaaa camdev']
b6[newcolsb6] = b6[colsb6].apply(lambda x: x.str.extract('(\d{4})', expand=False))

# Análisis de F5 
ica = InternalControlAnalysis(b6, index_name, cost_column)

dfcerr = b6[b6['estado_2']=='CERRADO']

dfcerr2 = ica.get_fnan( dfcerr, 'f5','F5')
dfcerr3  = ica.get_duplicates( dfcerr2, ['sku','cod.autorizacion'], 'sku  & cod.autorizacion')
ne = ica.get_notfound( dfcerr3, f5, ['f5','sku'], ['transfer','sku'], 'transfer', 'f5 & sku')
b6f5 = pd.merge(dfcerr3, f5, left_on=['f5','sku'], right_on=['transfer','sku'])
b6f52  = ica.get_diffvalue(b6f5, 'estado', 'Recibido', 'NRE', 'Registro con estado diferente a recibido')
b6f53 = ica.get_equalvalue(b6f52, 'motivo discrepancia', 'F5 NO RECIBIDO', 'MDNR', 'Registro con motivo de disc: F5 no recibido')
b6f54 = ica.get_diffqty(b6f53, 'unidades', 'cant. recibida','F5') # TODO actualizar qty a pro 
# TODO verificar año diferente a 2021 

iokf3 = b6f54[index_name].values
ica.update_db(iokf3, 'GCO', 'OKK')
ica.update_db(iokf3, 'Comentario GCO', 'Coincidencia exacta F5+UPC+QTY')
b6 = ica.get_db()

# Reporte 
print('\n ----------------- Base 6 ----------------- ')
print('\n ## Resumen de información según estado')
print(b6[['estado_2', cost_column]].groupby('estado_2').sum().sort_values(by=cost_column, ascending=False))

summaryres = b6[[cost_column, 'CIF5', 'estado_2']].groupby(['estado_2', 'CIF5']).agg(['sum', 'size']).sort_values(by=(cost_column,'sum'), ascending=False)
#reporte.print_analysis(comp='F5', comments='para estado cerrado', total= total, nan=nan, du=du, ne=nex, nr=nr, md=md, dc=ncc, summary=summaryres)

# Tareas finales 
b6.to_csv(f'output/{dt_string}-b6.csv', sep=';', decimal=',', index=False) # Guarda el archivo 
b6mf5 = b6.merge(f5, how='left', left_on=['f5','sku'], right_on=['transfer','sku'])
b6mf5.to_csv(f'output/{dt_string}-b6mf5.csv', sep=';', decimal=',', index=False)