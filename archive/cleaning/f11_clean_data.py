import pandas as pd 
from cl_cleaning import CleaningText as ct 
from datetime import datetime

dt_string = datetime.now().strftime('%y%m%d-%H%M')

nc = pd.read_excel('input/db-pbi/210823_f11.xlsx', sheet_name='DB',  dtype='object')

nc = ct.norm_header(nc)

path_intermedio = f'output/db-pbi/{dt_string}-data_f11-inter.csv'
nc.to_csv(path_intermedio, sep=';', index=False) 

#path_intermedio = f'output/db-pbi/210811-1923-data_f11-inter.csv'
nc = pd.read_csv(path_intermedio, sep=';', dtype='object')

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

nc = drop_except(nc, req_lista)

print('Limpiando texto en columnas')
nc.loc[:, text_lista] = nc.loc[:, text_lista].apply(ct.clean_str)

print('Convirtiendo a número parte 1')
nc.loc[:, fnum_lista] = nc.loc[:, fnum_lista].apply(ct.clean_fnum)

nc.fecha_corte =  pd.to_datetime(nc.fecha_corte)

nc.total_costo_promedio = pd.to_numeric(nc.total_costo_promedio)
nc.total_costo_promedio = round(nc.total_costo_promedio)
nc.total_costo_promedio = nc.total_costo_promedio.astype(int)

nc.costo_promedio = pd.to_numeric(nc.costo_promedio)
nc.costo_promedio = round(nc.costo_promedio)
nc.costo_promedio = nc.costo_promedio.fillna(0)
nc.costo_promedio = nc.costo_promedio.astype(int)

nc.to_csv(f'output/{dt_string}-data_f11.csv', sep=';', index=False) 