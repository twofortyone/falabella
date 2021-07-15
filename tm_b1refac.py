# Librerías
import pandas as pd
from datetime import datetime
from cl_cleaning import CleaningText as ct 
from ica_raw import InternalControlAnalysis

# Configuraciones
pd.set_option('float_format', '{:,.2f}'.format)

# Variables 
dt_string = datetime.now().strftime('%y%m%d-%H%M%S')
index_name = 'indice_b1'
cost_column = 'ct'

# Importar bases
refac = pd.read_csv('input/ajustes_3000/210528/210630-095612-refact.csv', sep=';', dtype='object')
b1 = pd.read_csv('input/ajustes_3000/210528/b1.csv', sep=';', dtype='object')

# Normalizar nombres de columnsa 
refac = ct.norm_header(refac)
b1 = ct.norm_header(b1)

# Convertir columnas de precio a dato numérico
b1 = ct.convertir_a_numero(b1, ['costo_unitario','costo_por_cantidad'])

# Generar indice en columna
b1.reset_index(inplace=True)
b1.rename(columns={'index': index_name}, inplace=True)
fcols = ['0','1','2','3','f12']

ica = InternalControlAnalysis(b1, index_name)
df1 = b1.copy()
print(df1.shape)
df2= ica.get_fnan( df1, fcols[4], 'F12')
print(df2.shape)
df3 = ica.get_duplicates( df2,[fcols[4],'upc', 'unidades'], 'F12 + UPC + Cantidad')
print(df3.shape)
ne = ica.get_notfound( df3, refac, [fcols[4]], ['f12cod'], 'f12cod', 'F12')
print(str(ne.shape)+ 'ne')
df4 = pd.merge(df3, refac, left_on=[fcols[4]], right_on=['f12cod'])
print(df4.shape)
df5 = ica.get_equalvalue(df4, 'confirmacion_tesoreria', 'NO REINTEGRADO - TRX DECLINADA', 'ANU', 'Registro con TRX declinada')
# df5 = cierres.ica.get_diffvalue(df4, 'estado', 'APPROVED', 'ANU', 'Registro con transacción anulada')
# df6 = cierres.ica.get_diffqty_pro(df5, 'qproducto', 'cantidad',f11_col, f3_col,'La cantidad sumada de los f11s de un f3 es mayor que la cantidad del f3')
iokf12 = df5[index_name].values
ica.update_db(iokf12,'GCO', 'OKK')
ica.update_db(iokf12,'Comentario GCO', 'Coincidencia exacta')

b2 = ica.get_db()

b2.loc[b2.GCO.notna(), 'checked'] = 'y'
b2.loc[b2.GCO.isna(), 'checked'] = 'n'


du = b2[b2.duplicated([fcols[4],'upc', 'unidades'], keep=False)]
idu = du[index_name].values
b2.loc[idu, 'DUP'] = 'y'
b2.loc[b2['DUP'].isna(), 'DUP'] = 'n'

b2.to_csv(f'output/{dt_string}-novedades-b1refact.csv', sep=';', index=False, encoding='utf-8') # Guarda el archivo 
b3 = pd.merge(b2, refac, left_on=[fcols[4]], right_on=['f12cod'], validate='many_to_one')

b3.to_csv(f'output/{dt_string}-novedades-b1refact-all.csv', sep=';', index=False, encoding='utf-8')