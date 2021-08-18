import pandas as pd 
from cl_cleaning import CleaningText as ct 
from datetime import datetime

dt_string = datetime.now().strftime('%y%m%d-%H%M')

f4 = pd.read_excel('input/Seguimiento_F4.xlsx', sheet_name='DATA',  dtype='object')
path_intermedio = f'output/db-pbi/{dt_string}-data_f4-inter.csv'
f4 = ct.norm_header(f4)
f4.to_csv(path_intermedio, sep=';', index=False) 

#path_intermedio = f'output/db-pbi/210811-1923-data_f11-inter.csv'
f4 = pd.read_csv(path_intermedio, sep=';', dtype='object')

def drop_except(df, cols):
    df.drop(df.columns.difference(cols), axis=1, inplace=True)
    return df 

req_lista = ['nfolio', 'nsecuencia', 'prd_upc', 'xsubprod', 'qproducto', 'csucursal', 'crut_clt', 'propietario',
       'xnombre_clt', 'xapellido_clt', 'xciudad_clt', 
       'dcreacion', 'xobservacion', 'dpactada_dt', 'dpactada_hd', 'ddespacho',
       'xestado',  'xservicio', 'borigen', 'suc', 'tienda', 'costo_promedio', 'total_costo_promedio',
       'fecha_corte', 'dias', 'antiguedad', 'grupo']

text_lista = ['xsubprod', 'csucursal', 'propietario', 'xnombre_clt', 'xapellido_clt',
       'xciudad_clt', 'xobservacion', 'xestado',  'xservicio', 'borigen',
       'tienda', 'antiguedad', 'grupo']

fnum_lista = ['nfolio', 'nsecuencia', 'prd_upc',  'crut_clt', 'suc', 'qproducto', 
          'suc','dias']

fechas = ['dcreacion','ddespacho', 'fecha_corte', 'dpactada_dt', 'dpactada_hd', 'ddespacho']

# 'costo_promedio', 'total_costo_promedio',

f4 = drop_except(f4, req_lista)

print('Limpiando texto en columnas')
f4.loc[:, text_lista] = f4.loc[:, text_lista].apply(ct.clean_str)

print('Convirtiendo a número parte 1')
f4.loc[:, fnum_lista] = f4.loc[:, fnum_lista].apply(ct.clean_fnum)

f4.fecha_corte =  pd.to_datetime(f4.fecha_corte)

f4.total_costo_promedio = pd.to_numeric(f4.total_costo_promedio)
f4.total_costo_promedio = round(f4.total_costo_promedio)
f4.total_costo_promedio = f4.total_costo_promedio.astype(int)

f4.costo_promedio = pd.to_numeric(f4.costo_promedio)
f4.costo_promedio = round(f4.costo_promedio)
f4.costo_promedio = f4.costo_promedio.fillna(0)
f4.costo_promedio = f4.costo_promedio.astype(int)

f4.to_csv(f'output/{dt_string}-data_f4.csv', sep=';', index=False) 