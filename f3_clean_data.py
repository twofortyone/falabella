import pandas as pd 
from cl_cleaning import CleaningText as ct 
from datetime import datetime

dt_string = datetime.now().strftime('%y%m%d-%H%M')

f3 = pd.read_excel('input/Seguimiento_F11.xlsx', sheet_name='data',  dtype='object')
path_intermedio = f'output/db-pbi/{dt_string}-data_f11-inter.csv'
f3 = ct.norm_header(f3)
f3.to_csv(path_intermedio, sep=';', index=False) 

#path_intermedio = f'output/db-pbi/210811-1923-data_f11-inter.csv'
f3 = pd.read_csv(path_intermedio, sep=';', dtype='object')

def drop_except(df, cols):
    df.drop(df.columns.difference(cols), axis=1, inplace=True)
    return df 

req_lista = ['nro_devolucion', 'nro_guia', 'tipo_producto', 'prd_upc', 'desc_linea',
       'desc_sclase', 'sku', 'desc_sku', 'rut_proveedor', 'dv_proveedor',
       'razon_social', 'local_envio', 'desc_local_envio', 'estado', 'fecha_reserva', 
       'fecha_envio', 'cantidad', 'cant_costo', 'cant_venta', 'tipo_documento_para_dev', 
       'folio_f11', 'folio_f12', 'antiguedad_reservado', 'antiguedad_enviado', 'dias', 'antiguedad', 'fecha_corte', 'grupo']

text_lista = ['tipo_producto', 'desc_linea', 'desc_sclase', 'desc_sku', 'razon_social', 
       'desc_local_envio',  'estado', 'tipo_documento_para_dev','antiguedad', 'grupo']

fnum_lista = ['nro_devolucion', 'nro_guia', 'prd_upc', 'sku', 'rut_proveedor', 'dv_proveedor', 'local_envio', 'folio_f11', 'folio_f12', 'antiguedad_reservado', 
       'antiguedad_enviado',  'dias']

fechas = [ 'fecha_reserva', 'fecha_aprobacion', 'fecha_envio', 'fecha_corte']

# 'cantidad', 'cant_costo', 'cant_venta',

f3 = drop_except(f3, req_lista)

print('Limpiando texto en columnas')
f3.loc[:, text_lista] = f3.loc[:, text_lista].apply(ct.clean_str)

print('Convirtiendo a número parte 1')
f3.loc[:, fnum_lista] = f3.loc[:, fnum_lista].apply(ct.clean_fnum)

f3.fecha_corte =  pd.to_datetime(f3.fecha_corte)

f3.cant_costo = pd.to_numeric(f3.cant_costo)
f3.cant_costo = round(f3.cant_costo)
f3.cant_costo = f3.cant_costo.astype(int)

f3.cant_venta = pd.to_numeric(f3.cant_venta)
f3.cant_venta = round(f3.cant_venta)
f3.cant_venta = f3.cant_venta.fillna(0)
f3.cant_venta = f3.cant_venta.astype(int)

f3.to_csv(f'output/{dt_string}-data_f3.csv', sep=';', index=False) 