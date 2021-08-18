import pandas as pd
from datetime import datetime
from ica_nc import CierresNC
from tqdm import tqdm
#import input.tipmc as intip

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
index_name = 'indice_f5'
cost_column = 'ct'
status_column = 'tipmc'
qty_column = 'cantidad_trx_actual'
upc_column = 'upc'
fcols = ['f3','f4','f5','f11', 'f12', 'cod_aut_nc']
# --------------------------------------------------------------------------------------------
# Cargar data
data = []
names = ['f3', 'f4', 'f5', 'kpi','refact', 'cierres_nc']

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

colsf3 = ['fecha_reserva', 'fecha_envio', 'fecha_anulacion','fecha_confirmacion']
newcolsf3 = ['aaaa reserva', 'aaaa envio', 'aaaa anulacion','aaaa confirmacion']
f3[newcolsf3] = f3[colsf3].apply(lambda x: x.str.extract('(\d{4})', expand=False))

# Convertir columnas a fecha 
kpi['fecha_paletiza'] = pd.to_datetime(kpi['fecha_paletiza'])

# TODO ---- revisar hasta aquí 

# Inicio de análisis de cierres 
cerrado = nc[nc['esmc']=='cerrado']
cierres_nc = CierresNC(nc, index_name)
cierres_nc.set_fcols(fcols, [status_column, upc_column, cost_column, qty_column])
#TODO fix f12 number in f4_verify

# Validación B6
cierres_nc.f5_verify_20(f5, '2021', 'cod_aut_nc')
lista_tipm_f4 = ['se asocia f4 dado de baja por producto entregado a cliente','se asocia f4 por producto no ubicado','se asocia f4-baja de inventario-menaje']
print('Análisis F4s')
for tipo2 in tqdm(lista_tipm_f4):
    cierres_nc.f4_verify_20_b6(f4, tipo2, '2021')

# Validación B2345
cierres_nc.f3_verify_20(f3, 'f3 devuelto a proveedor', '2021')

lista_tipm_f4 = ['f4 de merma', 'f4 cobrado a terceros' ]
print('Análisis F4s')
for tipo2 in tqdm(lista_tipm_f4):
    cierres_nc.f4_verify_20(f4, tipo2, '2021')

cierres_nc.kpi_verify_20_2435(kpi, '2021', 'Recibido con fecha anterior al 21/01/2021')

# Validación B1 
cierres_nc.refact_verify_20(refact)

# Validación B7
cierres_nc.kpi_verify_20(kpi, '2021', 'Recibido con fecha anterior al 21/01/2021')

nc = cierres_nc.get_db()
nc.loc[nc['GCO'].notna(), 'checked'] = 'y'
nc.loc[nc['GCO'].isna(), 'checked'] = 'n'

# Identificar duplicados en toda la base 
du = nc[nc.duplicated(['cod_aut_nc', 'upc', 'cantidad_trx_actual'], keep=False)]
idu = du[index_name].values
nc.loc[idu, 'DUP'] = 'Y'
nc.loc[nc['DUP'].isna(), 'DUP'] = 'N'
print(nc.groupby('DUP')[cost_column].sum())

print(cerrado[[status_column, cost_column]].groupby(status_column).sum().sort_values(by=cost_column, ascending=False))

res = nc.groupby([status_column,'GCO']).agg({cost_column:['sum', 'count']}).sort_values([status_column,('ct','sum')],ascending=False)
print(res)
print(res[('ct', 'sum')].sum())

def guardar():
    nc.to_excel(f'output/cierres_nc/{dt_string}-cnc-output.xlsx', sheet_name=f'{dt_string}_cnc', index=False)
    nc2 = nc.merge(f5, how='left', left_on=[fcols[2],upc_column], right_on=['transfer','upc'], validate='many_to_one')
    nc3 = nc2.merge(f4, how='left',  left_on=[fcols[1],upc_column], right_on=['nro_red_inventario','upc'],validate='many_to_one')
    path = f'output/cierres_nc/{dt_string}-cnc-all.xlsx'
    nc3.to_excel(path, sheet_name=f'{dt_string}_cnc', index=False) 
    return path

print('Desea guardar los resultados? (y/n)')
save_res = input('//:')

if save_res=='y':
    path = guardar()
    print(f'Guardado como: {path}')
else:
    print('Ok')