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
index_name = 'indice_b1'
cost_column = 'costo por cantidad'

b1 = pd.read_csv('input/base1.csv', sep=';', dtype='object')
ref = pd.read_csv('input/kpi.csv', sep=';', dtype='object')