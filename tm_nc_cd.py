from os import dup
import pandas as pd
from datetime import datetime
import numpy as np
from cl_cleaning import CleaningText as ct 
from ica_nc import InternalControlAnalysis
from report import Report 
from unidecode import unidecode
from tqdm import tqdm

pd.set_option('float_format', '{:,.2f}'.format)
pd.set_option('display.max_columns', 70)

dt_string = datetime.now().strftime('%y%m%d-%H%M%S')

cts = ['9913','9917','9919','9920','9918','9912','9914']
preventas = ['9904','9905','9908','9909','9915','9916']
tiendas = ['139','141','142','143','5','37','35','43','53','36','38','98','138',
'45','72','183','25','123','19','50','85','101','321','323','324','108','6','18','82',
'93','322','13','56','96','131','60']
# --------------------------------------------------------------------------------------------
# TODO Check values 
index_name = 'indice_f5'
cost_column = 'ct'
status_column = 'tipmc'
qty_column = 'cantidad_trx_actual'
upc_column = 'upc'
f5_col = 'f5'
f3_col = 'f3'
f4_col = 'f4'
f11_col= 'f11'
# --------------------------------------------------------------------------------------------
f5 = pd.read_csv(f'input/nc_3000/210617/210610-162143-f5-output.csv', sep=';', dtype='object')
f4 = pd.read_csv(f'input/nc_3000/210617/210616-100307-f4-output.csv', sep=';', dtype='object')
kpi = pd.read_csv(f'input/nc_3000/210617/210616-101000-kpi.csv', sep=';', dtype='object')
nc = pd.read_csv(f'input/nc_3000/210617/210617_nc.csv', sep=';', dtype='object')
f3 = pd.read_csv(f'input/nc_3000/210617/210616-082924-f3.csv', sep=';', dtype='object')
refact = pd.read_csv(f'input/nc_3000/210617/210616-refact.csv', sep=';', dtype='object')

nc.drop(nc.columns[77:108], axis=1, inplace=True)
f5 = ct.normalizar_cols(f5)
f4 = ct.normalizar_cols(f4)
f3 = ct.normalizar_cols(f3)
kpi = ct.normalizar_cols(kpi)
nc = ct.normalizar_cols(nc)
refact = ct.normalizar_cols(refact)

f5 = ct.convertir_a_numero(f5, ['cant. recibida'])
f4 = ct.convertir_a_numero(f4, ['cantidad'])
nc = ct.convertir_a_numero(nc, [cost_column,'cantidad_trx_actual'])

#TODO cambiar fechas de texto a date 
colsf5 = ['fe. reserva', 'fe. envo', 'fe. recep']
newcolsf5 = ['aaaa reserva', 'aaaa envio', 'aaaa recep']
f5[newcolsf5] = f5[colsf5].apply(lambda x: x.str.extract('(\d{4})', expand=False))

f4['aa creacion'] = f4['fecha creacion'].str.split('-').str[2]

nc.drop(['foto', 'llave_f12', 'llave_oc', 'llave_so', 'duplicado', 
        'fecha_proceso','terminal', 'secuencia','transaccion', 'boleta', 
        'fecha_proc_ant', 'terminal_ant','secuencia_ant', 'cod_trx_ant', 
        'boleta_ant','ctip_prd','xtip_prd', 'desc_sku', 'cantidad_trx_anterior',
        'costo_prmd', 'costo_rep', 'monto_trx_actual','monto_trx_anterior', 
        'efectivo_nc', 'oc_act', 'oc_ant','primera_do_f12', 'crut_co', 'xdvrut_co', 
        'nrut_rcp', 'xdvrut_rcp','bretira_dsp', 'retira_cliente', 'nsuborden', 
        'local_ab', 'local_via','fecha_eta', 'cestado', 'estado_f12_srx', 'csubestado',
        'suestado_dx','cproblema', 'xnoentrega','tipo_caso', 'dif_dias', 'tipificacion', 
        'subtipo', 'f5.1', 'f3.1','f4.1', 'f11.1','motivo_cierre', 'subtipo.1'], axis=1, inplace=True)

f4.drop([ 'local', 'desc. local', 'tipo red.inv', 'usuario creacion', 'fecha reserva',
       'usuario reserva', 'fecha envio', 'usuario envio', 'destino',
       'r.u.t destino', 'centro de costos', 'desc.centro e costo', 'linea',
        'subclase', 'descripcion subclase','nro. producto', 'precio vta', 
       'precio costo', 'total precio vta', 'total precio costo',], axis=1, inplace = True)

f3.drop([ 'nro guia', 'tipo producto', 'marca', 'subclase', 'descripcion.1', 'clase',
       'descripcion.2', 'sublinea', 'descripcion.3', 'linea', 'descripcion.4',
       'proveedor', 'rut proveedor', 'descripcion.5', 'estado', 'cant*costo', 'cant*costoprmd',
       'diferencia', 'cant*precio', 'tipo documento para dev',
       'usuario que confirma', 'nc proveedor'], axis=1, inplace=True )

nc.reset_index(inplace=True)
nc.rename(columns={'index': index_name}, inplace=True)
cerrado = nc[nc['esmc']=='CERRADO']

ica = InternalControlAnalysis(nc, index_name, cost_column, status_column, qty_column, upc_column)
ica.set_fcols([])
ica.set_fcols([f3_col, f4_col, f5_col, f11_col,'','cod_aut_nc'])
#TODO fix f12 number in f4_verify

def cierre_f5(bd, status):
    df1 = bd[bd[status_column]==status]
    df2 = ica.get_fnan( df1, f5_col, 'F5')
    df3 = ica.get_duplicates( df2, ['cod_aut_nc', 'local_trx','upc', 'cantidad_trx_actual'], 'Cod Aut + Local + UPC + Qty')
    ne = ica.get_notfound( df3, f5, [f5_col,'upc'], ['transfer','upc'], 'transfer', 'F5|UPC|Qty')
    df4 = pd.merge(df3, f5, left_on=[f5_col,'upc'], right_on=['transfer','upc'])
    df5 = ica.get_diffvalue(df4, 'estado_y', 'Recibido', 'NRE', 'Registro con estado diferente a recibido')
    df6 = ica.get_equalvalue(df5, 'motivo discrepancia', 'F5 NO RECIBIDO', 'MDI', 'Registro con motivo de disc: F5 no recibido')
    df7 = ica.get_diffvalue(df6, 'aaaa reserva', '2021', 'NAA', 'Registro con año de reserva diferente a 2021')
    comment = 'La cantidad sumada de los cod. aut. nc de un F5 es mayor que la cantidad del F5'
    df8 = ica.get_diffqty_pro(df7, 'cantidad_trx_actual', 'cant. recibida', 'cod_aut_nc', 'transfer', comment)
    iokf5 = df8[index_name].values
    ica.update_db(iokf5, 'GCO','OKK')
    ica.update_db(iokf5, 'Comentario GCO', 'Coincidencia exacta F5+UPC+QTY')
    #print('-----------------------------------')
    #print(f'## {status}')
    #print(nc[[cost_column,'GCO']].groupby(['GCO']).agg(['sum', 'count']).sort_values(by=(cost_column,'sum'), ascending=False))

def cierre_f4(bd, status):
    df1 = bd[bd[status_column]==status]
    df2 = ica.get_fnan( df1, f4_col, 'F4')
    df3 = ica.get_duplicates( df2, ['cod_aut_nc', 'local_trx','upc', 'cantidad_trx_actual'], 'Cod Aut + Local + UPC + Qty')
    ne = ica.get_notfound( df3, f4, [f4_col,'upc'], ['nro. red. inventario','upc'], 'nro. red. inventario', 'F4|UPC|Qty')
    df4 = pd.merge(df3, f4, left_on=[f4_col,'upc'], right_on=['nro. red. inventario','upc'])
    df5 = ica.get_equalvalue(df4, 'estado_y', 'Anulado', 'ANU', 'Registro con estado anulado')
    df6 = ica.get_diffvalue(df5, 'aa creacion', '2021', 'NAA', 'Registro con año de creación diferente a 2021')
    comment = 'La cantidad sumada de los cod. aut. nc de un F4 es mayor que la cantidad del F4'
    df7 = ica.get_diffqty_pro(df6, 'cantidad_trx_actual', 'cantidad','cod_aut_nc', f4_col, comment)
    iokf5 = df7[index_name].values
    ica.update_db(iokf5, 'GCO','OKK')
    ica.update_db(iokf5, 'Comentario GCO', 'Coincidencia exacta F5+UPC+QTY') 

lista_tipmc_f5 = ['CON MC ASOCIADA','COMPENSACIÓN CON CT VERDE','SE ASOCIA F11-CONCILIACION CON TRANSPORTADORA',
'CON QUIEBRE ASOCIADO','CON F11 TIPO CLIENTE ASOCIADO','SE ASOCIA F3-DEVUELTO A PROVEEDOR',
'CON RO ASOCIADO','COMPENSA CON LOCAL DE VENTA/ANULADO X USER', 'F12 EN DIGITADO SIN SALIDA']
print('Análisis F5s')
for tipmc in tqdm(lista_tipmc_f5): 
    #cierre_f5(cerrado, tipmc)
    ica.f5_verify(f5, tipmc, '2021', 'cod_aut_nc')

lista_tipm_f4 = ['SE ASOCIA F4-BAJA DE INVENTARIO-MENAJE', 'BAJA CON CARGO A LINEA POR COSTOS']
print('Análisis F4s')
for tipmc2 in tqdm(lista_tipm_f4):
    #cierre_f4(cerrado, tipmc2)
    ica.f4_verify(f4, tipmc2, '2021')

ica.f5_verify_local(f5, 'COMPENSACIÓN CON DVD ADMINISTRATIVO', '2021', 'cod_aut_nc', '3001')
ica.f5_verify_local_list(f5, 'COMPENSACIÓN CON CT CIUDADES', '2021', 'cod_aut_nc', cts)
ica.f5_verify_local_list(f5,'COMPENSACIÓN CON PREVENTAS', '2021', 'cod_aut_nc', preventas)
ica.f5_verify_local_list(f5,'COMPENSACIÓN CON TIENDA', '2021', 'cod_aut_nc', tiendas)
#ica.get_duplicates(nc, ['cod_aut_nc', 'local_trx','upc', 'cantidad_trx_actual'], 'Cod Aut + Local + UPC + Qty')

nc = ica.get_db()
print(cerrado[[status_column, cost_column]].groupby(status_column).sum().sort_values(by=cost_column, ascending=False))

res = nc.groupby([status_column,'GCO']).agg({cost_column:['sum', 'count']}).sort_values([status_column,('ct','sum')],ascending=False)
print(res)
#print(res[('ct', 'sum')].sum())
#nc.to_csv(f'output/{dt_string}-nc-output.csv', sep=';', index=False)