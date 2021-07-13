# Librerías
import pandas as pd
from datetime import datetime
from cl_cleaning import CleaningText as ct 
from ica_cierres import CierresF11
from unidecode import unidecode
import numpy as np 

# Configuraciones
pd.set_option('float_format', '{:,.2f}'.format)

# Cargar data
data = []
names = ['f3', 'f4', 'f5', 'kpi','refact', 'cf11_cd_20']

for name in names:
    data.append(pd.read_csv(f'input/cierres_f11s/210712/210712-211321-{name}.csv', sep=';', dtype='object'))

f3, f4, f5, kpi, refact, cf11 = data[0],data[1],data[2],data[3],data[4],data[5]

# Variables 
dt_string = datetime.now().strftime('%y%m%d-%H%M%S')
index_name = 'indice_cf11'
cost_column = 'total_costo_promedio'
status_column = 'status_nuevo'
qty_column = 'qproducto'
upc_column = 'prd_upc'
fcols = ['f3nuevo','f4_nuevo','f5','nfolio','f12']

# Generar indice en columna
cf11.reset_index(inplace=True)
cf11.rename(columns={'index': index_name}, inplace=True)

# Convertir columnas a número 
f3.loc[:,'cantidad'] = pd.to_numeric(f3.loc[:,'cantidad'])
f4.loc[:,'cantidad'] = pd.to_numeric(f4.loc[:,'cantidad'])
f5.loc[:,'cant_pickeada'] = pd.to_numeric(f5.loc[:,'cant_pickeada'])
f5.loc[:,'cant_recibida'] = pd.to_numeric(f5.loc[:,'cant_recibida'])
#f5.loc[:,['cant_pickeada','cant_recibida']] =  f5[['cant_pickeada','cant_recibida']].apply(pd.to_numeric)
cf11.loc[:,['qproducto','total_costo_promedio']] = cf11[['qproducto','total_costo_promedio']].apply(pd.to_numeric)

# Convertir columnas a fecha 
kpi['fecha_paletiza'] = pd.to_datetime(kpi['fecha_paletiza'])

# TODO ---- revisar desde aquí 
colsf5 = ['fe_reserva', 'fe_envo', 'fe_recep']
newcolsf5 = ['aaaa reserva', 'aaaa envio', 'aaaa recep']
f5[newcolsf5] = f5[colsf5].apply(lambda x: x.str.extract('(\d{4})', expand=False))
# Obtener el año de la reserva, el envío y la recepción
# datecolsf4 = ['fecha creacion',  'fecha reserva', 'fecha envio']
# newdatecolf4 = ['aa creacion',  'aa reserva', 'aa envio']
# f4[newdatecolf4] = f4[datecolsf4].apply(lambda x: x.str.extract('(\d{2})', expand=False))
# TODO Pasar esto a limpieza F4 
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
cf11 = cierres.ica.get_db()

# 30 de junio de 2021 
# Comparar duplicados con los de michael 
# TODO  pasar a método 
concept1 = 'cierre x duplicidad (f11 con mismo f12+sku+cantidad)'
#concept2 = 'registro duplicado en base de datos'
#sin_cat_dup = cf11[(cf11['status_nuevo']!= concept1)&(cf11['status_nuevo']!=concept2)]
sin_cat_dup = cf11[cf11['status_nuevo']!= concept1] # No es categoría dup

cat_dup = cf11[cf11['status_nuevo']== concept1] # Es categoría dup
#cat_dup = cf11[((cf11['status_nuevo']== concept1)|(cf11['status_nuevo']==concept2))]

dup_cols = ['f12', 'prd_upc']
cat_dup_mas_gco = cat_dup[cat_dup['gco_dup']=='y'] # Duplicados para MC y GCO
redcols = ['f12', 'prd_upc']
redcols.append('status_nuevo')
#redcols.append(index_name)
cat_dup_mas_gco = cat_dup_mas_gco[redcols]

cat_dup_mas_gco.drop_duplicates(dup_cols, inplace=True)

cf11.loc[cat_dup_mas_gco.index, 'dupmc'] = 'y'

mdup = pd.merge(sin_cat_dup, cat_dup_mas_gco, on=dup_cols,validate='many_to_one') # Registros unicos MC de duplicados
print(mdup.empty)
cf11.loc[mdup['indice_cf11'].values, 'dupmc'] = 'y'

aux = mdup[mdup.duplicated(dup_cols)]
cf11.loc[aux['indice_cf11'].values, 'dupmc'] = np.nan
cf11.loc[aux['indice_cf11'].values,'error_ru'] = 'y'

cf11.loc[(cf11['dupmc'].isna())& (cf11['gco_dup'] =='y') ,'gco_dupall'] = 'y'
cf11.loc[(cf11['dupmc'].isna())& (cf11['gco_dup'] =='y') & (cf11.GCO =='OKK'),'Comentario GCO'] = 'Coincidencia exacta + Registro duplicado en DB'

print(cf11.groupby('gco_dup')[cost_column].sum())
print(cf11.groupby('gco_dupall')[cost_column].sum())

res = cf11.groupby([status_column,'GCO']).agg({cost_column:['sum', 'size']}).sort_values(by=[status_column,(cost_column,'sum')], ascending=False)
print(res)# Presenta todos los estados 

def guardar():
    cf11.to_excel(f'output/{dt_string}-novedades-cf11s_cd_20.xlsx', sheet_name=f'{dt_string}_cf11_cd_20', index=False, encoding='utf-8') # Guarda el archivo 
    bdcia = cf11.merge(f3, how='left', left_on=[fcols[0],'prd_upc'], right_on=['nro_devolucion','upc'], validate='many_to_one')
    bdcia2 = bdcia.merge(f4, how='left',  left_on=[fcols[1],'prd_upc'], right_on=['nro_red_inventario','upc'],validate='many_to_one')
    bdcia3 = bdcia2.merge(f5, how='left', left_on=[fcols[2],'prd_upc'], right_on=['transfer','upc'], validate='many_to_one')
    bdcia4 = bdcia3.merge(kpi, how='left',left_on=[fcols[3]], right_on=['entrada'],validate='many_to_one')
    bdcia5 = bdcia4.merge(kpi, how='left',left_on=[fcols[4]], right_on=['entrada'],validate='many_to_one')
    #bdcia6 = bdcia4.merge(refact, how='left',left_on=[fcols[4]], right_on=['f12cod'],validate='many_to_one')
    bdcia5.to_excel(f'output/{dt_string}-novedades-cf11s_cd_20-all.xlsx', sheet_name=f'{dt_string}_cf11_cd_20',index=False) 

print('Desea guardar los resultados? (y/n)')
save_res = input('//:')

if save_res=='y':
    guardar()
    print(f'Guardado como: {dt_string}-novedades-cf11s_cd_20.xlsx')
else:
    print('Ok')