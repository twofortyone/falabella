# Librerías
import pandas as pd
from datetime import datetime
from unidecode import unidecode
import numpy as np
from cleaning import CleaningText as ct 
from ica import InternalControlAnalysis
from report import Report 

# Configuraciones
pd.set_option('float_format', '{:,.2f}'.format)

# Variables 
dt_string = datetime.now().strftime('%y%m%d-%H%M%S')
index_name = 'indice_b7'
cost_column = 'ct'

# Importar bases
#refac = pd.read_csv('input/.csv', sep=';', dtype='object')
b1 = pd.read_csv('input/base1.csv', sep=';', dtype='object')

# Normalizar nombres de columnsa 
#kpi = ct.normalizar_cols(kpi)
b1 = ct.normalizar_cols(b1)

# Convertir columnas de precio a dato numérico
b1 = ct.convertir_a_numero(b1, ['costo unitario','costo por cantidad'])

# Generar indice en columna
b1.reset_index(inplace=True)
b1.rename(columns={'index': index_name}, inplace=True)

print(b1.columns)