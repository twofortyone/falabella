# Librerías
import pandas as pd
from datetime import datetime
from cl_cleaning import CleaningText as ct 
from ica_cierres_tienda import CierresF11
from report import Report 
from unidecode import unidecode
import numpy as np 

# Configuraciones
pd.set_option('float_format', '{:,.2f}'.format)

# Cargar data
data = []
names = ['f3', 'f4', 'f5', 'kpi','refact', 'cf11_tienda']

for name in names:
    data.append(pd.read_csv(f'input/cierres_f11s/210623_tienda/210623-123027-{name}.csv', sep=';', dtype='object'))

f3, f4, f5, kpi, refact, c11t = data[0],data[1],data[2],data[3],data[4],data[5]

# Variables 
dt_string = datetime.now().strftime('%y%m%d-%H%M%S')
index_name = 'indice_c11t'
cost_column = 'total_costo_promedio'
status_column = 'motivo'
qty_column = 'qproducto'
upc_column = 'prd_upc'
fcols = ['f','f','f','nfolio','f']
fcolaux = ['f', 'nfolio']

# Generar indice en columna
c11t.reset_index(inplace=True)
c11t.rename(columns={'index': index_name}, inplace=True)

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

# Tareas finales 
cierres.finals()
c11t = cierres.ica.get_db()

res = c11t.groupby([status_column,'GCO']).agg({cost_column:['sum', 'size']}).sort_values(by=[status_column,(cost_column,'sum')], ascending=False)
print(res)# Presenta todos los estados 

def guardar():
    c11t.to_csv(f'output/{dt_string}-cf11_tienda.csv', sep=';', index=False) # Guarda el archivo 
    # bdcia = c11t.merge(f3, how='left', left_on=[fcols[0],'prd_upc'], right_on=['nro_devolucion','upc'], validate='many_to_one')
    # bdcia2 = bdcia.merge(f4, how='left',  left_on=[fcols[1],'prd_upc'], right_on=['nro_red_inventario','upc'],validate='many_to_one')
    # bdcia3 = bdcia2.merge(f5, how='left', left_on=[fcols[2],'prd_upc'], right_on=['transfer','upc'], validate='many_to_one')
    # bdcia4 = bdcia3.merge(kpi, how='left',left_on=[fcols[3]], right_on=['entrada'],validate='many_to_one')
    # bdcia5 = bdcia4.merge(refact, how='left',left_on=[fcols[4]], right_on=['f12cod'],validate='many_to_one')
    # bdcia4.to_csv(f'output/{dt_string}-novedades-cierref11-all.csv', sep=';', index=False) 
 
guardar()
