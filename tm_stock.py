# Librerías
import pandas as pd
from datetime import datetime
from cl_cleaning import CleaningText as ct 

# Configuraciones
pd.set_option('float_format', '{:,.2f}'.format)

# Variables 
dt_string = datetime.now().strftime('%y%m%d-%H%M%S')
#names = ['LOCAL', 'LINEA', 'DESCRIP.LINEA', 'SUBINEA', 'DESCRIP.SUBL', 'CLASE',
#       'DESCRIP.CLASE', 'SUBCLASE', 'DESCRIP.SUBCLASE', 'MARCA', 'NUMBER',
#       'UPC', 'DESCRIP.PRODUCTO', 'P.VTA', 'STOCK DISPONIBLE', 'UN']
#s3000 = pd.read_csv(f'input/book5s3000.csv', sep=';', dtype='object', header=None, names=names)
s3000 = pd.read_csv(f'input/book5s3000.csv', sep=';', dtype='object')

#s3000 = pd.read_csv(f'input/210608_s3000.txt', sep=';', dtype='object')
s3000 = ct.convertir_a_numero(s3000, ['STOCK DISPONIBLE'])
upcs300 = s3000[['LINEA','UPC', 'STOCK DISPONIBLE']].groupby('UPC').sum().reset_index()

cnc = pd.read_csv(f'input/210608_cnc.csv', sep=';', dtype='object')
abierto = cnc[cnc['ESMC']!='Cerrado']
abierto = ct.convertir_a_numero(abierto, ['CANTIDAD_TRX_ACTUAL'])
upcsabierto = abierto[['UPC', 'CANTIDAD_TRX_ACTUAL']].groupby('UPC').sum().reset_index()

#merge = upcs300.merge(upcsabierto, on=['UPC'])
#merge.to_csv(f'output/{dt_string}-stockout.csv', sep=';', decimal=',', index=False) 

