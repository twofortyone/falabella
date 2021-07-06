# Librerías
import pandas as pd
from datetime import datetime
from cl_cleaning import CleaningText as ct 
from ica_cierres import CierresF11
from report import Report 
from unidecode import unidecode
import numpy as np 

# Configuraciones
pd.set_option('float_format', '{:,.2f}'.format)

# Cargar data
data = []
names = ['f3', 'f4', 'f5', 'kpi','refact', 'cierre_f11']

for name in names:
    data.append(pd.read_csv(f'input/cierres_f11s/210630/210630-095612-{name}.csv', sep=';', dtype='object'))

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

f3['cantidad'] = f3['cantidad'].fillna('N/A')
f3.loc[~f3.cantidad.str.isdigit(),'cantidad'] = np.nan 
f3 = ct.convertir_a_numero(f3, ['cantidad']) # Convertir columnas de precio a dato numérico
f4['cantidad'] = f4['cantidad'].fillna('N/A')
f4.loc[~f4.cantidad.str.isdigit(),'cantidad'] = np.nan 
f4 = ct.convertir_a_numero(f4, ['cantidad']) # Convertir columnas de precio a dato numérico
f5 = ct.convertir_a_numero(f5, ['cant_recibida','cant_pickeada'])
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

# Toma los campos de las Fs y les asigna nan a los que no sean númericos 
for f in fcols:
    aux = bc11[bc11[f].notna()]
    indaux = aux[~aux[f].str.isdigit()][index_name].values
    bc11.loc[indaux, f] = np.nan

# TODO ---- revisar hasta aquí 

# Inicio de análisis de cierres 
cierres = CierresF11(bc11, index_name)
cierres.set_fcols(fcols, [status_column, upc_column, cost_column, qty_column])

cols = [fcols[4],upc_column, qty_column]
cierres.starting(cols)

#cierres.f4_verify(f4, 'f4 de merma-2020', '2020')
lista_f4_2021 = ['f4 en revision','cierre x f4 cobrado a terceros', 'f4 de merma', 'error en cierre de f11', 'error en creacion de f11','politica cambio agil','cierre x f4 dado baja crate prestamos']
for status_nuevo in lista_f4_2021:
    cierres.f4_verify(f4, status_nuevo, '2021')

lista_f3_2021 = ['f3 en revision','cierre x f3 devuelto a proveedor']
for status_nuevo_f3 in lista_f3_2021:
    cierres.f3_verify(f3, status_nuevo_f3, '2021')

cierres.f5_verify(f5, 'producto en tienda', '2021')
cierres.kpi_verify(kpi, 'cierre x producto guardado despues de inventario', '2021', 'Recibido con fecha anterior al 21/01/2021')
cierres.kpi_verify(kpi, 'cierre x producto guardado antes de inventario', '2020', 'Recibido con fecha posterior al 20/01/2021')
cierres.refact_verify(refact, 'cierre x recupero con cliente - refacturacion - base fal.com')

# Tareas finales
cierres.finals()
bc11 = cierres.ica.get_db()

# 30 de junio de 2021 
# Comparar duplicados con los de michael 

concept1 = 'cierre x duplicidad (f11 con mismo f12+sku+cantidad)'
concept2 = 'registro duplicado en base de datos'
sin_cat_dup = bc11[(bc11['status_nuevo']!= concept1)&(bc11['status_nuevo']!=concept2)]

cat_dup = bc11[((bc11['status_nuevo']== concept1)|(bc11['status_nuevo']==concept2))]

dup_cols = ['f12', 'prd_upc']
cat_dup_mas_gco = cat_dup[cat_dup['gco_dup']=='y'] # Duplicados para MC y GCO
redcols = dup_cols
redcols.append('status_nuevo')
redcols.append(index_name)
cat_dup_mas_gco = cat_dup_mas_gco[redcols]
cat_dup_mas_gco.drop_duplicates(dup_cols, inplace=True)

bc11.loc[cat_dup_mas_gco['indice_bc11'].values, 'dupmc'] = 'y'

mdup = pd.merge(sin_cat_dup, cat_dup_mas_gco, on=dup_cols,validate='many_to_one') # Registros unicos MC de duplicados
bc11.loc[mdup['indice_bc11'].values, 'dupmc'] = 'y'

aux = mdup[mdup.duplicated(dup_cols)]
bc11.loc[aux['indice_bc11'].values, 'dupmc'] = np.nan
bc11.loc[aux['indice_bc11'].values,'error_ru'] = 'y'

bc11.loc[(bc11['dupmc'].isna())& (bc11['gco_dup'] =='y') ,'gco_dupall'] = 'y'
bc11.loc[(bc11['dupmc'].isna())& (bc11['gco_dup'] =='y') & (bc11.GCO =='OKK'),'Comentario GCO'] = 'Coincidencia exacta + Registro duplicado en DB'

print(bc11.groupby('gco_dup')[cost_column].sum())
print(bc11.groupby('gco_dupall')[cost_column].sum())

res = bc11.groupby([status_column,'GCO']).agg({cost_column:['sum', 'size']}).sort_values(by=[status_column,(cost_column,'sum')], ascending=False)
print(res)# Presenta todos los estados 

def guardar():
    bc11.to_csv(f'output/{dt_string}-novedades-cierref11.csv', sep=';', index=False, encoding='utf-8') # Guarda el archivo 
    bdcia = bc11.merge(f3, how='left', left_on=[fcols[0],'prd_upc'], right_on=['nro_devolucion','upc'], validate='many_to_one')
    bdcia2 = bdcia.merge(f4, how='left',  left_on=[fcols[1],'prd_upc'], right_on=['nro_red_inventario','upc'],validate='many_to_one')
    bdcia3 = bdcia2.merge(f5, how='left', left_on=[fcols[2],'prd_upc'], right_on=['transfer','upc'], validate='many_to_one')
    bdcia4 = bdcia3.merge(kpi, how='left',left_on=[fcols[3]], right_on=['entrada'],validate='many_to_one')
    bdcia5 = bdcia4.merge(kpi, how='left',left_on=[fcols[4]], right_on=['entrada'],validate='many_to_one')
    #bdcia6 = bdcia4.merge(refact, how='left',left_on=[fcols[4]], right_on=['f12cod'],validate='many_to_one')
    bdcia5.to_csv(f'output/{dt_string}-novedades-cierref11-all.csv', sep=';', index=False) 
 
#guardar()
