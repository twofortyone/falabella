# Librerías
import pandas as pd
from datetime import datetime
from ica_core.ica_cierres_tienda import CierresF11

# Configuraciones
pd.set_option('float_format', '{:,.2f}'.format)

# Cargar data
data = []
names = ['f3', 'f4', 'f5', 'kpi','refact', 'cf11_tienda_21']

pre_file = input('Ingrese prefijo de archivos: ')

for name in names:
    data.append(pd.read_csv(f'input/cierres_f11/tienda/{pre_file}{name}.csv', sep=';', dtype='object'))

f3, f4, f5, kpi, refact, c11t = data[0],data[1],data[2],data[3],data[4],data[5]

# Variables 
dt_string = datetime.now().strftime('%y%m%d-%H%M')
index_name = 'indice_c11t'
cost_column = 'costo_promedio'
status_column = 'motivo_cierre'
qty_column = 'cantidad_f11'
upc_column = 'ean'
fcols = ['f','f','f','folio_servicio_tecnico','f']
fcolaux = ['f', 'folio_servicio_tecnico']

# Generar indice en columna
c11t.reset_index(inplace=True)
c11t.rename(columns={'index': index_name}, inplace=True)
#c11t.rename(columns={'estado': 'estado_f11'}, inplace=True)

# Convertir columnas a número 
f3.loc[:,'cantidad'] = pd.to_numeric(f3.loc[:,'cantidad'])
f4.loc[:,'cantidad'] = pd.to_numeric(f4.loc[:,'cantidad'])
f5.loc[:,'cant_pickeada'] = pd.to_numeric(f5.loc[:,'cant_pickeada'])
f5.loc[:,'cant_recibida'] = pd.to_numeric(f5.loc[:,'cant_recibida'])
#f5.loc[:,['cant_pickeada','cant_recibida']] =  f5[['cant_pickeada','cant_recibida']].apply(pd.to_numeric)
c11t.loc[:,[qty_column,cost_column]] = c11t[[qty_column,cost_column]].apply(pd.to_numeric)

# TODO ---- revisar desde aquí 

#c11t.prd_upc= c11t.prd_upc.str.split('.').str[0] # Limpiar la columna de upc 

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
cierres = CierresF11(c11t, index_name)
cierres.set_fcols(fcols, [status_column, upc_column, cost_column, qty_column])

lista_f4_2021 = ['f4']
for status_nuevo in lista_f4_2021:
    cierres.f4_verify(f4, status_nuevo, '2021')

lista_f3_2021 = ['f3']
for status_nuevo_f3 in lista_f3_2021:
    cierres.f3_verify(f3, status_nuevo_f3, '2021')

cierres.f5_verify(f5, 'f5', '2021')

# Tareas finales 
cierres.finals()
c11t = cierres.ica.get_db()

res = c11t.groupby([status_column,'GCO']).agg({cost_column:['sum', 'size']}).sort_values(by=[status_column,(cost_column,'sum')], ascending=False)
print(res)# Presenta todos los estados 

def guardar():
    path = f'output/cierres_f11/tienda/{dt_string}-cf11_tienda_21-output.xlsx'
    c11t.to_excel(path, sheet_name=f'{dt_string}-cf11_tienda', index=False) # Guarda el archivo 
    return path
 
print('Desea guardar los resultados? (y/n)')
save_res = input('//:')

if save_res=='y':
    path = guardar()
    print(f'Guardado en: {path}')
else:
    print('Ok')
