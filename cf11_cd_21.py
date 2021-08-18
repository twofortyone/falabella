# Librerías
import pandas as pd
from datetime import datetime
from cl_cleaning import CleaningText as ct 
from ica_cierres import CierresF11

# Configuraciones
pd.set_option('float_format', '{:,.2f}'.format)

# Cargar data
data = []
names = ['f3', 'f4', 'f5', 'kpi','refact', 'cf11_cd_21']

pre_file = input('Ingrese prefijo de archivos: ')

for name in names:
    data.append(pd.read_csv(f'input/cierres_f11/cd/{pre_file}{name}.csv', sep=';', dtype='object'))

f3, f4, f5, kpi, refact, cf11 = data[0],data[1],data[2],data[3],data[4],data[5]

cf11 = cf11.rename(columns={'f11':'nfolio'})
# Variables 
dt_string = datetime.now().strftime('%y%m%d-%H%M')
index_name = 'indice_cf11'
cost_column = 'costo_total'
status_column = 'status_final'
qty_column = 'qproducto'
upc_column = 'prd_upc'
fcols = ['f3','f4','f5','nfolio','f12']

# Generar indice en columna
cf11.reset_index(inplace=True)
cf11.rename(columns={'index': index_name}, inplace=True)

# Convertir columnas a número 
f3.loc[:,'cantidad'] = pd.to_numeric(f3.loc[:,'cantidad'])
f4.loc[:,'cantidad'] = pd.to_numeric(f4.loc[:,'cantidad'])
f5.loc[:,'cant_pickeada'] = pd.to_numeric(f5.loc[:,'cant_pickeada'])
f5.loc[:,'cant_recibida'] = pd.to_numeric(f5.loc[:,'cant_recibida'])
#f5.loc[:,['cant_pickeada', 'cant_recibida']] = f5.loc[:,['cant_pickeada', 'cant_recibida']].apply(pd.to_numeric)
cf11.loc[:,['qproducto','costo_total']] = cf11.loc[:,['qproducto','costo_total']].apply(pd.to_numeric)

# TODO ---- revisar desde aquí 
#cf11.prd_upc= cf11.prd_upc.str.split('.').str[0] # Limpiar la columna de upc 

# TODO Fin primera etapa 
kpi['fecha_paletiza'] = pd.to_datetime(kpi['fecha_paletiza'])
colsf5 = ['fe_reserva', 'fe_envo', 'fe_recep']
newcolsf5 = ['aaaa reserva', 'aaaa envio', 'aaaa recep']
f5[newcolsf5] = f5[colsf5].apply(lambda x: x.str.extract('(\d{4})', expand=False))
# Obtener el año de la reserva, el envío y la recepción
# datecolsf4 = ['fecha creacion',  'fecha reserva', 'fecha envio']
# newdatecolf4 = ['aa creacion',  'aa reserva', 'aa envio']
# f4[newdatecolf4] = f4[datecolsf4].apply(lambda x: x.str.extract('(\d{2})', expand=False))
# TODO Pasar esto a limpieza F4 y lo de borrar lineas de txt 
colsf3 = ['fecha_reserva', 'fecha_envio', 'fecha_anulacion','fecha_confirmacion']
newcolsf3 = ['aaaa reserva', 'aaaa envio', 'aaaa anulacion','aaaa confirmacion']
f3[newcolsf3] = f3[colsf3].apply(lambda x: x.str.extract('(\d{4})', expand=False))

f4['aa creacion'] = f4['fecha_creacion'].str.split('-').str[2]
# TODO ---- revisar hasta aquí 

# Inicio de análisis de cierres 
cierres = CierresF11(cf11, index_name)
cierres.set_fcols(fcols, [status_column, upc_column, cost_column, qty_column])

cols = [fcols[4],upc_column, qty_column]
cierres.starting(cols)

#cierres.f4_verify(f4, 'f4 de merma-2020', '2020')
lista_f4_2021 = ['f4 en revision','cierre x f4 cobrado a terceros', 'f4 de merma', 'error en cierre de f11', 'error en creacion de f11'] 
for status_nuevo in lista_f4_2021:
    cierres.f4_verify(f4, status_nuevo, '2021')

lista_f3_2021 = ['f3 en revision','cierre x f3 devuelto a proveedor']
for status_nuevo_f3 in lista_f3_2021:
    cierres.f3_verify(f3, status_nuevo_f3, '2021')

cierres.f5_verify(f5, 'producto en tienda', '2021')
cierres.kpi_verify(kpi, 'cierre x producto guardado despues de inventario', '2021', 'Recibido antes del 21/Enero/2021')
cierres.kpi_verify(kpi, 'cierre x producto guardado antes de inventario', '2020', 'Recibido después del 20/Enero/2021')
cierres.refact_verify(refact, 'cierre x recupero con cliente - refacturacion - base fal.com')

# Tareas finales 
cierres.finals()
cf11 = cierres.ica.get_db()

print(cf11.groupby('gco_dup')[cost_column].sum())

res = cf11.groupby([status_column,'GCO']).agg({cost_column:['sum', 'size']}).sort_values(by=[status_column,(cost_column,'sum')], ascending=False)
print(res)# Presenta todos los estados 

def guardar():
    cf11.to_excel(f'output/cierres_f11/cd/{dt_string}-cf11_cd_21-output.xlsx', sheet_name=f'{dt_string}_cf11_cd_21', index=False, encoding='utf-8') # Guarda el archivo 
    bdcia = cf11.merge(f3, how='left', left_on=[fcols[0],'prd_upc'], right_on=['nro_devolucion','upc'], validate='many_to_one')
    bdcia2 = bdcia.merge(f4, how='left',  left_on=[fcols[1],'prd_upc'], right_on=['nro_red_inventario','upc'],validate='many_to_one')
    bdcia3 = bdcia2.merge(f5, how='left', left_on=[fcols[2],'prd_upc'], right_on=['transfer','upc'], validate='many_to_one')
    bdcia4 = bdcia3.merge(kpi, how='left',left_on=[fcols[3]], right_on=['entrada'],validate='many_to_one')
    bdcia5 = bdcia4.merge(kpi, how='left',left_on=[fcols[4]], right_on=['entrada'],validate='many_to_one')
    #bdcia5 = bdcia4.merge(refact, how='left',left_on=[fcols[4]], right_on=['f12cod'],validate='many_to_one')
    path = f'output/cierres_f11/cd/{dt_string}-cf11_cd_21-all.xlsx'
    bdcia5.to_excel(path, sheet_name=f'{dt_string}_cf11_cd_21', index=False) 
    return path
 
print('Desea guardar los resultados? (y/n)')
save_res = input('//:')

if save_res=='y':
    path = guardar()
    print(f'Guardado en: {path}')
else:
    print('Ok')