import pandas as pd 
from cl_cleaning import CleaningText as ct 
from datetime import datetime

dt_string = datetime.now().strftime('%y%m%d-%H%M')

f3_ant = 'input/bases/210825_data_seguimiento_f3.csv'
f3_nvo = 'output/planillas/210830-1157-f3-output.csv'

f4_ant = 'input/bases/210825_data_seguimiento_f4.csv'
f4_nvo = 'output/planillas/210830-1158-f4-output.csv'


def drop_except(df, cols):
              df.drop(df.columns.difference(cols), axis=1, inplace=True)
              return df 

def get_antiguedad(celda):

              if (celda>=0) & (celda<16):
                     return'1.entre 0 y 15 dias'
              elif (celda>=16) & (celda<31):
                     return '2.entre 16 y 30 dias'
              elif (celda>=31) & (celda<61):
                     return '3.entre 31 y 60 dias'
              elif (celda>=61) & (celda<91):
                     return '4.entre 61 y 90 dias'
              elif (celda>=91) & (celda<181):
                     return '5.entre 91 y 180 dias'
              elif (celda>=181) & (celda<361):
                     return '6.entre 181 y 360 dias'
              else:
                     return '7.mas de 360 dias'

def replace_date(col):
              col = col.str.replace('ene', '01')
              col = col.str.replace('feb', '02')
              col = col.str.replace('mar', '03')
              col = col.str.replace('abr', '04')
              col = col.str.replace('may', '05')
              col = col.str.replace('jun', '06')
              col = col.str.replace('jul', '07')
              col = col.str.replace('ago', '08')
              col = col.str.replace('sep', '09')
              col = col.str.replace('oct', '10')
              col = col.str.replace('nov', '11')
              col = col.str.replace('dic', '12')
              return col  

def na_date(col):
              col.fillna('01-01-1990', inplace = True)
              return col        

# F3s -------------------------------------------------------------------------------------------------------------------------------------------

f3 = pd.read_csv(f3_ant, sep=';', dtype='object')
tre = pd.read_csv(f3_nvo, sep=';', dtype='object')

tre = tre.rename(columns={'upc':'prd_upc', 'descripcion':'desc_sku' , 'descripcion1':'desc_sclase', 'descripcion4':'desc_linea', 
       'proveedor':'razon_social', 'local':'local_envio', 'descripcion5':'desc_local_envio', 'estado':'estado_num', 
       'descripcion6':'estado', 'cant*costo':'cant_costo', 'cant*precio':'cant_venta' })

f3req_lista = ['nro_devolucion', 'nro_guia', 'tipo_producto', 'prd_upc', 'desc_linea',
       'desc_sclase', 'sku', 'desc_sku', 'rut_proveedor', 'dv_proveedor',
       'razon_social', 'local_envio', 'desc_local_envio', 'estado', 'fecha_reserva', 
       'fecha_envio', 'cantidad', 'cant_costo', 'cant_venta', 'tipo_documento_para_dev', 
       'folio_f11', 'folio_f12']

fechas = [ 'fecha_reserva', 'fecha_envio']

tre = drop_except(tre, f3req_lista)

tre = tre.loc[(tre.estado=='reservado') | (tre.estado=='enviado') ]
tre['fecha_corte'] = pd.to_datetime('today').normalize()
tre.fecha_corte = pd.to_datetime(tre.fecha_corte)

tre.loc[:, fechas] = tre.loc[:, fechas].apply(na_date)
tre.loc[:, fechas] = tre.loc[:, fechas].apply(replace_date)

tre.fecha_reserva = pd.to_datetime(tre.fecha_reserva)
tre.fecha_envio = pd.to_datetime(tre.fecha_envio)

delta_reservado = tre.fecha_corte - tre.fecha_reserva 
delta_envio = tre.fecha_corte - tre.fecha_envio

tre['antiguedad_reservado'] =  delta_reservado.dt.days
tre['antiguedad_enviado'] = delta_envio.dt.days

tre.loc[tre.estado=='reservado', 'dias'] = tre.loc[tre.estado=='reservado', 'antiguedad_reservado']
tre.loc[tre.estado=='enviado', 'dias'] = tre.loc[tre.estado=='enviado', 'antiguedad_enviado']
tre['antiguedad'] = tre.dias.apply(get_antiguedad)

tsave = pd.concat([f3, tre], axis=0)
tsave.to_csv(f'output/{dt_string}-data_seguimiento_f3.csv', sep=';', index=False) 

# F4s -------------------------------------------------------------------------------------------------------------------------------------------

f4_req_lista = ['nro_red_inventario', 'local', 'desc_local', 'estado', 'tipo_redinv',
       'fecha_creacion', 'usuario_creacion', 'fecha_reserva',
       'usuario_reserva', 'fecha_envio', 'usuario_envio', 'destino',
       'rut_destino', 'centro_de_costos', 'desccentro_e_costo', 'linea',
       'descripcion_linea', 'subclase', 'descripcion_subclase', 'nro_producto',
       'descripcion_producto', 'upc', 'cantidad', 'precio_vta', 'precio_costo',
       'total_precio_vta', 'total_precio_costo']

tiendas = ['72', '18', '93', '36', '25', '53', '50', '35', '123', '45', '131', '43', '56', '101', '98', '85',
'19', '108', '183', '60', '82', '6', '138', '96', '38', '13', '37', '5', '322', '141']
cd = ['9903', '2000', '9951', '2001', '2002', '9921', '9961', '3000', '9919', '9970', '9905']

f4 = pd.read_csv(f4_ant, sep=';', dtype='object')
cua = pd.read_csv(f4_nvo, sep=';', dtype='object')

cua = drop_except(cua, f4_req_lista)

cua['fecha_corte'] = pd.to_datetime('today').normalize()

cua.loc[:, fechas] = cua.loc[:, fechas].apply(na_date)
cua.loc[:, fechas] = cua.loc[:, fechas].apply(replace_date)

cua.loc[cua.local.isin(tiendas), 'grupo'] = 'tienda'
cua.loc[cua.local.isin(cd), 'grupo'] = 'cd'

cua.loc[cua.local =='9911', 'grupo'] = 'bodega producto en proceso'
cua.loc[cua.local =='3001', 'grupo'] = 'dvd administrativo'
cua.loc[cua.local =='3004', 'grupo'] = 'express'
cua.loc[cua.local =='11', 'grupo'] = 'venta empresa'

cua.loc[cua.local =='99', 'grupo'] = 'administrativo'
cua.loc[cua.local =='9910', 'grupo'] = 'bodega mavesa'

cua = cua.loc[cua.estado!='anulado']

fsave = pd.concat([f4, cua], axis=0)
fsave.to_csv(f'output/{dt_string}-data_seguimiento_f4.csv', sep=';', index=False)


#  F11s -------------------------------------------------------------------------------------------------------------------------------------------

""" 
f11 = pd.read_csv('', sep=';', dtype='object')


f3_req_lista = ['nro_devolucion', 'nro_guia', 'tipo_producto', 'prd_upc', 'desc_linea',
       'desc_sclase', 'sku', 'desc_sku', 'rut_proveedor', 'dv_proveedor',
       'razon_social', 'local_envio', 'desc_local_envio', 'estado', 'fecha_reserva', 
       'fecha_envio', 'cantidad', 'cant_costo', 'cant_venta', 'tipo_documento_para_dev', 
       'folio_f11', 'folio_f12']

fechas = [ 'fecha_reserva', 'fecha_envio']

f3 = drop_except(f3, f3_req_lista)

f11 = f11.loc[(f11.estado=='reservado') | (f11.estado=='enviado') ]
f11['fecha_corte'] = pd.to_datetime('today').normalize()
f11.fecha_corte = pd.to_datetime(f11.fecha_corte)

f11.loc[:, fechas] = f11.loc[:, fechas].apply(na_date)
f11.loc[:, fechas] = f11.loc[:, fechas].apply(replace_date)

f11.fecha_reserva = pd.to_datetime(f11.fecha_reserva, format='%d-%m-%Y')
f11.fecha_envio = pd.to_datetime(f11.fecha_envio, format='%d-%m-%Y')

delta= f11.fecha_corte - f11.fecha_reserva 
f11['antiguedad'] =  delta.dt.days

f11.loc[f11.estado=='reservado', 'dias'] = f11.loc[f11.estado=='reservado', 'antiguedad_reservado']
f11.loc[f11.estado=='enviado', 'dias'] = f11.loc[f11.estado=='enviado', 'antiguedad_enviado']
f11['antiguedad'] = f11.dias.apply(get_antiguedad)

#tsave = pd.concat([f3, f11], axis=0)
#tsave.to_csv(f'output/{dt_string}-data_f3.csv', sep=';', index=False) 
f11.to_csv(f'output/{dt_string}-data_f3.csv', sep=';', index=False)  """