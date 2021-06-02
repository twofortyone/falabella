# Librerías
import pandas as pd
from datetime import datetime

# Variables
now = datetime.now()
dt_string = now.strftime("%y%m%d-%H%M%S")

# Lee el archivo F4 original
f4 = pd.read_csv('input/cierresf11s/f40602.csv', sep=';', dtype='object')

shape_v1 = f4.shape  # Obtiene la dimensión del dataframe

# Elimina las filas dos o menos valores no nulos
vacias = f4[f4.isnull().sum(axis=1) >= (shape_v1[1]-2)]
vacias.to_csv(dt_string + '-f4-vacias.csv', sep=';')
f4.dropna(thresh=2, inplace=True)
n_vacias = shape_v1[0]-f4.shape[0]

# Identifica las entradas en Nro. Red. Inventario nulas y reemplazar por texto
f4['Nro. Red. Inventario'].fillna('N/A', inplace=True)

# Identifica los registros desplazados
rd = f4[(~f4['Nro. Red. Inventario'].str.isdigit()) | (f4['Nro. Red. Inventario'].str.startswith('1'))]
rd.to_csv(dt_string + '-f4-desplazadas.csv', sep=';')

# Ajusta los valores desplazados
indice = rd.index
faux = f4.copy()

for i in indice:
    f4.loc[i-1, 'Destino':'Total Precio Costo'] = faux.loc[i,'Nro. Red. Inventario':'Linea'].values

f4 = f4[(f4['Nro. Red. Inventario'].str.isdigit()) & (~f4['Nro. Red. Inventario'].str.startswith('1'))]

# Extraer el F11
f4['F11'] = f4.Destino.str.extract('(\d{9})')  # Extrae el valor F11
f4.to_csv(dt_string + '-f4-output.csv', sep=';', index=False)
num_fonces = f4['F11'].notna().sum()

print(f'Se encontró: \n {n_vacias} filas con dos o menos valores no nulos \n {indice.shape[0]}  registros desplazados \n{num_fonces} registros con valores de F11')
