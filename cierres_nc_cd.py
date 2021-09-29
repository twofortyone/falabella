import pandas as pd
from datetime import datetime
from ica_core.ica_nc import CierresNC
from tqdm import tqdm
import input.tipmc as intip

pd.set_option('float_format', '{:,.2f}'.format)
pd.set_option('display.max_columns', 70)

dt_string = datetime.now().strftime('%y%m%d-%H%M')

cts = ['9913','9917','9919','9920','9918','9912','9914', '9902']
preventas = ['9904','9905','9908','9909','9915','9916']
tiendas = ['139','141','142','143','5','37','35','43','53','36','38','98','138',
'45','72','183','25','123','19','50','85','101','321','323','324','108','6','18','82',
'93','322','13','56','96','131','60']
# --------------------------------------------------------------------------------------------
# TODO Check values 
index_name = 'indice_cnc'
cost_column = 'ct'
status_column = 'tipificacion_final'
qty_column = 'cantidad_trx_actual'
upc_column = 'upc'
estado_col = 'estado_final'
fcols = ['f3','f4','f5','f11', '', 'cod_aut_nc']
# --------------------------------------------------------------------------------------------
# Cargar data
data = []
names = ['f3', 'f4', 'f5', 'kpi','refact', 'cierres_nc_21']

pre_file = input('Ingrese prefijo de archivos: ')

for name in names:
    data.append(pd.read_csv(f'input/cierres_nc/{pre_file}{name}.csv', sep=';', dtype='object'))

f3, f4, f5, kpi, refact, nc = data[0],data[1],data[2],data[3],data[4],data[5]

# Generar indice en columna
nc.reset_index(inplace=True)
nc.rename(columns={'index': index_name}, inplace=True)

""" # TODO ARREGLAR 
nc.loc[status_column == 'compensaciin con ct verde', status_column] = 'compensacion con ct verde'
nc.loc[status_column == 'compensaciin con tienda', status_column] = 'compensacion con tienda'
nc.loc[status_column == 'compensaciin con preventas', status_column] = 'compensacion con preventas'
nc.loc[(status_column == 'compensa con ro')|(status_column == 'compensa con ro asociado'), status_column] = 'con ro asociado'
 """
# Convertir columnas a número 
f3.loc[:,'cantidad'] = pd.to_numeric(f3.loc[:,'cantidad'])
f4.loc[:,'cantidad'] = pd.to_numeric(f4.loc[:,'cantidad'])
f5.loc[:,'cant_pickeada'] = pd.to_numeric(f5.loc[:,'cant_pickeada'])
f5.loc[:,'cant_recibida'] = pd.to_numeric(f5.loc[:,'cant_recibida'])
#f5.loc[:,['cant_pickeada','cant_recibida']] =  f5[['cant_pickeada','cant_recibida']].apply(pd.to_numeric)
nc.loc[:,['cantidad_trx_actual', cost_column]] = nc[['cantidad_trx_actual', cost_column]].apply(pd.to_numeric)

# TODO ---- revisar desde aquí 
#TODO cambiar fechas de texto a date 
colsf5 = ['fe_reserva', 'fe_envo', 'fe_recep']
newcolsf5 = ['aaaa reserva', 'aaaa envio', 'aaaa recep']
f5[newcolsf5] = f5[colsf5].apply(lambda x: x.str.extract('(\d{4})', expand=False))

f4['aa creacion'] = f4['fecha_creacion'].str.split('-').str[2]
# TODO ---- revisar hasta aquí 

# Inicio de análisis de cierres 
cerrado = nc[nc[estado_col]=='cerrado']
cierres_nc = CierresNC(nc)
cierres_nc.set_fcols(fcols, [index_name, status_column, upc_column, cost_column, qty_column, estado_col])
#TODO fix f12 number in f4_verify

lista_tipmc_f5 = [ 'con mc asociada','con ro asociado','compensacion con ct verde', 'con quiebre asociado', 'f5 en revision','se asocia f11-conciliacion con transportadora',
'con f11 tipo cliente asociado','se asocia f3-devuelto a proveedor','compensa con local de ventaanulado x user', 'f12 en digitado sin salida']

print('Análisis F5s')
for tipo in tqdm(lista_tipmc_f5):
    cierres_nc.f5_verify(f5, tipo, '2021')

lista_tipm_f4 = [ 'se asocia f4 dado de baja por producto entregado a cliente', 'se asocia f4 por producto no ubicado','se asocia f4-baja de inventario-menaje',
 'baja con cargo a linea por costos', 'baja con cargo a dependencia por politicasdefiniciones', 'error en generacion de nota credito', 'se asocia f4-baja de inventario-fast track', 'se asocia f4 por producto no ubicado - postventa']
print('Análisis F4s')
for tipo2 in tqdm(lista_tipm_f4):
    cierres_nc.f4_verify(f4, tipo2, '2021')

cierres_nc.f5_verify_local(f5, 'compensacion con dvd administrativo', '2021', '3001')
print('cts ------------------------------------------------------------')
nil = cierres_nc.f5_verify_local_list(f5, 'compensacion con ct ciudades', '2021', 'CTs',cts)
print(nil)
print('preventas ------------------------------------------------------------')
nil = cierres_nc.f5_verify_local_list(f5,'compensacion con preventas', '2021',  'preventas',preventas)
print(nil)
print('tiendas ------------------------------------------------------------')
nil = cierres_nc.f5_verify_local_list(f5,'compensacion con tienda', '2021',  'tiendas ',tiendas)
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
    nc.to_excel(f'output/cierres_nc/{dt_string}-cnc_21-output.xlsx', sheet_name=f'{dt_string}_cnc', index=False)
    nc2 = nc.merge(f5, how='left', left_on=[fcols[2],upc_column], right_on=['transfer','upc'], validate='many_to_one')
    nc3 = nc2.merge(f4, how='left',  left_on=[fcols[1],upc_column], right_on=['nro_red_inventario','upc'],validate='many_to_one')
    path = f'output/cierres_nc/{dt_string}-cnc_21-all.xlsx'
    nc3.to_excel(path, sheet_name=f'{dt_string}_cnc', index=False) 
    return path

print('Desea guardar los resultados? (y/n)')
save_res = input('//:')

if save_res=='y':
    path = guardar()
    print(f'Guardado en: {path}')
else:
    print('Ok')