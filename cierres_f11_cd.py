# Librerías
import pandas as pd
from datetime import datetime
from cl_cleaning import CleaningText as ct 
from ica_cierres import CierresF11
from report import Report 
from unidecode import unidecode

# Configuraciones
pd.set_option('float_format', '{:,.2f}'.format)

# Cargar data
data = []
names = ['f3', 'f4', 'f5', 'kpi','refact', 'cierre_f11']

for name in names:
    data.append(pd.read_csv(f'input/cierres_f11s/210623/210623-094858-{name}.csv', sep=';', dtype='object'))

f3, f4, f5, kpi, refact, bc11 = data[0],data[1],data[2],data[3],data[4],data[5]

# Variables 
dt_string = datetime.now().strftime('%y%m%d-%H%M%S')
index_name = 'indice_bc11'
cost_column = 'total_costo_promedio'
status_column = 'status_nuevo'
qty_column = 'qproducto'
upc_column = 'prd_upc'
fcols = ['f3nuevo','f4_nuevo','f5','nfolio','f12']

# TODO ---- revisar desde aquí 
 
bc11 = ct.limpiar_cols(bc11, fcols)
bc11 = ct.limpiar_cols(bc11, [status_column])
f4 = ct.limpiar_cols(f4, ['nro_red_inventario','upc', 'cantidad'])

bc11 = ct.convertir_a_numero(bc11, [cost_column, 'qproducto']) # Convertir columnas de precio a dato numérico
bc11.prd_upc= bc11.prd_upc.str.split('.').str[0] # Limpiar la columna de upc 

f4 = ct.convertir_a_numero(f4, ['cantidad']) # Convertir columnas de precio a dato numérico
f3 = ct.convertir_a_numero(f3, ['cantidad']) # Convertir columnas de precio a dato numérico

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

# Generar indice en columna
bc11.reset_index(inplace=True)
bc11.rename(columns={'index': index_name}, inplace=True)
bc11[status_column] = bc11[status_column].fillna('N/A')
bc11[status_column] = bc11[status_column].apply(unidecode)

# TODO ---- revisar hasta aquí 

# Inicio de análisis de cierres 
cierres = CierresF11(bc11, index_name)
cierres.set_fcols(fcols, [status_column, upc_column, cost_column, qty_column])

#cierres.f4_verify(f4, 'f4 de merma-2020', '2020')
lista_f4_2021 = ['f4 en revision','cierre x f4 cobrado a terceros', 'f4 de merma', 'error en cierre de f11', 'error en creacion de f11','politica cambio agil','cierre x f4 dado baja crate prestamos']
for status_nuevo in lista_f4_2021:
    cierres.f4_verify(f4, status_nuevo, '2021')

lista_f3_2021 = ['f3 en revision','cierre x f3 devuelto a proveedor']
for status_nuevo_f3 in lista_f3_2021:
    cierres.f3_verify(f3, status_nuevo_f3, '2021')

cierres.f5_verify(f5, 'producto en tienda', '2021')
cierres.kpi_verify(kpi, 'cierre x producto guardado despues de inventario', '2021', 'Recibido antes de 2021')
cierres.kpi_verify(kpi, 'cierre x producto guardado antes de inventario', '0000', 'Recibido después del 20 de enero de 2021')
cierres.refact_verify(refact, 'cierre x recupero con cliente - refacturacion - base fal.com')

# Tareas finales 
cierres.finals()
bc11 = cierres.ica.get_db()

du = bc11[bc11.duplicated([fcols[4],upc_column, qty_column], keep=False)]
idu = du[index_name].values
bc11.loc[idu, 'DUP'] = 'Y'
bc11.loc[bc11['DUP'].isna(), 'DUP'] = 'N'
print(bc11.groupby('DUP')[cost_column].sum())

res = bc11.groupby([status_column,'GCO']).agg({cost_column:['sum', 'size']}).sort_values(by=[status_column,(cost_column,'sum')], ascending=False)
print(res)# Presenta todos los estados 

def guardar():
    bc11.to_csv(f'output/{dt_string}-novedades-cierref11.csv', sep=';', index=False, encoding='utf-8') # Guarda el archivo 
    bdcia = bc11.merge(f3, how='left', left_on=[fcols[0],'prd_upc'], right_on=['nro_devolucion','upc'], validate='many_to_one')
    bdcia2 = bdcia.merge(f4, how='left',  left_on=[fcols[1],'prd_upc'], right_on=['nro_red_inventario','upc'],validate='many_to_one')
    bdcia3 = bdcia2.merge(f5, how='left', left_on=[fcols[2],'prd_upc'], right_on=['transfer','upc'], validate='many_to_one')
    bdcia4 = bdcia3.merge(kpi, how='left',left_on=[fcols[3]], right_on=['entrada'],validate='many_to_one')
    #bdcia5 = bdcia4.merge(refact, how='left',left_on=[fcols[4]], right_on=['f12cod'],validate='many_to_one')
    bdcia4.to_csv(f'output/{dt_string}-novedades-cierref11-all.csv', sep=';', index=False) 
 
guardar()
