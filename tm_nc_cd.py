import pandas as pd
from datetime import datetime
from cl_cleaning import CleaningText as ct 
from ica_nc import CierresNC
from tqdm import tqdm

pd.set_option('float_format', '{:,.2f}'.format)
pd.set_option('display.max_columns', 70)

dt_string = datetime.now().strftime('%y%m%d-%H%M%S')

cts = ['9913','9917','9919','9920','9918','9912','9914', '9902']
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
fcols = ['f3','f4','f5','f11', '', 'cod_aut_nc']
# --------------------------------------------------------------------------------------------
# Cargar data
data = []
names = ['f3', 'f4', 'f5', 'kpi','refact', 'cierre_nc']

for name in names:
    data.append(pd.read_csv(f'input/nc_3000/210625/210625-110311-{name}.csv', sep=';', dtype='object'))

f3, f4, f5, kpi, refact, nc = data[0],data[1],data[2],data[3],data[4],data[5]

nc.drop(nc.columns[77:108], axis=1, inplace=True)

f5 = ct.convertir_a_numero(f5, ['cant_recibida', 'cant_pickeada'])
f4 = ct.convertir_a_numero(f4, ['cantidad'])
nc = ct.convertir_a_numero(nc, [cost_column,'cantidad_trx_actual'])

nc = ct.limpiar_cols(nc, [status_column, 'esmc'])
#nc[status_column] = nc[status_column].apply(unidecode)

#TODO cambiar fechas de texto a date 
colsf5 = ['fe_reserva', 'fe_envo', 'fe_recep']
newcolsf5 = ['aaaa reserva', 'aaaa envio', 'aaaa recep']
f5[newcolsf5] = f5[colsf5].apply(lambda x: x.str.extract('(\d{4})', expand=False))

f4['aa creacion'] = f4['fecha_creacion'].str.split('-').str[2]

nc.reset_index(inplace=True)
nc.rename(columns={'index': index_name}, inplace=True)
cerrado = nc[nc['esmc']=='cerrado']
#cerrado.loc[:,status_column] = cerrado.loc[:,status_column].apply(unidecode)

cierres_nc = CierresNC(nc, index_name)
cierres_nc.set_fcols(fcols, [status_column, upc_column, cost_column, qty_column])
#TODO fix f12 number in f4_verify

lista_tipmc_f5 = ['f5 en revisión', 'con mc asociada','compensación con ct verde','se asocia f11-conciliacion con transportadora',
'con quiebre asociado','con f11 tipo cliente asociado','se asocia f3-devuelto a proveedor',
'con ro asociado','compensa con local de venta/anulado x user', 'f12 en digitado sin salida']

print('Análisis F5s')
for tipo in tqdm(lista_tipmc_f5):
    cierres_nc.f5_verify(f5, tipo, '2021', 'cod_aut_nc')

lista_tipm_f4 = ['se asocia f4-baja de inventario-menaje', 'baja con cargo a linea por costos', 'se asocia f4-baja de inventario-menaje / en revisión f4']
print('Análisis F4s')
for tipo2 in tqdm(lista_tipm_f4):
    cierres_nc.f4_verify(f4, tipo2, '2021')

cierres_nc.f5_verify_local(f5, 'compensación con dvd administrativo', '2021', 'cod_aut_nc', '3001')
print('cts ------------------------------------------------------------')
nil = cierres_nc.f5_verify_local_list(f5, 'compensación con ct ciudades', '2021', 'cod_aut_nc', 'CTs',cts)
print(nil)
print('preventas ------------------------------------------------------------')
nil = cierres_nc.f5_verify_local_list(f5,'compensación con preventas', '2021', 'cod_aut_nc', 'preventas',preventas)
print(nil)
print('tiendas ------------------------------------------------------------')
nil = cierres_nc.f5_verify_local_list(f5,'compensación con tienda', '2021', 'cod_aut_nc', 'tiendas ',tiendas)
print(nil)

nc = cierres_nc.get_db()
nc.loc[nc['GCO'].notna(), 'checked'] = 'y'
nc.loc[nc['GCO'].isna(), 'checked'] = 'n'

# Identificar duplicados en toda la base 
du = nc[nc.duplicated(['cod_aut_nc', 'local_trx','upc', 'cantidad_trx_actual'], keep=False)]
idu = du[index_name].values
nc.loc[idu, 'DUP'] = 'Y'
nc.loc[nc['DUP'].isna(), 'DUP'] = 'N'
print(nc.groupby('DUP')[cost_column].sum())

print(cerrado[[status_column, cost_column]].groupby(status_column).sum().sort_values(by=cost_column, ascending=False))

res = nc.groupby([status_column,'GCO']).agg({cost_column:['sum', 'count']}).sort_values([status_column,('ct','sum')],ascending=False)
print(res)
print(res[('ct', 'sum')].sum())

def guardar():
    nc.to_csv(f'output/{dt_string}-nc-output.csv', sep=';', index=False)
    nc2 = nc.merge(f5, how='left', left_on=[fcols[2],upc_column], right_on=['transfer','upc'], validate='many_to_one')
    nc3 = nc2.merge(f4, how='left',  left_on=[fcols[1],upc_column], right_on=['nro_red_inventario','upc'],validate='many_to_one')
    nc3.to_csv(f'output/{dt_string}-cierres-nc-all.csv', sep=';', index=False) 

#guardar()