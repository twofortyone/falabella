import pandas as pd
from datetime import datetime
import numpy as np
from cleaning import CleaningText as ct 
from ica import InternalControlAnalysis
from report import Report 
from unidecode import unidecode
from tqdm import tqdm

pd.set_option('float_format', '{:,.2f}'.format)
pd.set_option('display.max_columns', 70)

dt_string = datetime.now().strftime('%y%m%d-%H%M%S')
index_name = 'indice_f5'
cost_column = 'ct'
status_column = 'tipmc'

f5 = pd.read_csv(f'input/210610-162143-f5-output.csv', sep=';', dtype='object')
nc = pd.read_csv(f'input/210610-nc.csv', sep=';', dtype='object')
nc.drop(nc.columns[77:108], axis=1, inplace=True)
f5 = ct.normalizar_cols(f5)
nc = ct.normalizar_cols(nc)
nc = ct.convertir_a_numero(nc, [cost_column])

#TODO cambiar fechas de texto a date 
colsf5 = ['fe. reserva', 'fe. envo', 'fe. recep']
newcolsf5 = ['aaaa reserva', 'aaaa envio', 'aaaa recep']
f5[newcolsf5] = f5[colsf5].apply(lambda x: x.str.extract('(\d{4})', expand=False))

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

nc.reset_index(inplace=True)
nc.rename(columns={'index': index_name}, inplace=True)
cerrado = nc[nc['esmc']=='CERRADO']

ica = InternalControlAnalysis(nc, index_name, cost_column)

print(cerrado[[status_column, cost_column]].groupby(status_column).sum().sort_values(by=cost_column, ascending=False))

f5_col = 'f5'

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

nc = ica.get_db()

lista_tipmc_f5 = ['CON MC ASOCIADA','COMPENSACIÓN CON CT VERDE','SE ASOCIA F11-CONCILIACION CON TRANSPORTADORA','CON QUIEBRE ASOCIADO','CON F11 TIPO CLIENTE ASOCIADO','COMPENSACIÓN CON TIENDA','SE ASOCIA F3-DEVUELTO A PROVEEDOR','CON RO ASOCIADO','COMPENSACIÓN CON DVD ADMINISTRATIVO','COMPENSACIÓN CON PREVENTAS','COMPENSA CON LOCAL DE VENTA/ANULADO X USER','COMPENSACIÓN CON CT CIUDADES']

for tipmc in tqdm(lista_tipmc_f5): 
    cierre_f5(cerrado, tipmc)

res = nc.groupby([status_column,'GCO']).agg({cost_column:['sum', 'count']}).sort_values([status_column,('ct','sum')],ascending=False)
print(res)

nc.to_csv(f'output/{dt_string}-nc-output.csv', sep=';', index=False)