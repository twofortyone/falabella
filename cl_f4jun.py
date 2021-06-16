import pandas as pd
from datetime import datetime
import numpy as np

# Verificar para cada archivo 
num_f4_files = 2
f4_input_name = '210616_f4' # Prefijo del nombre del archivo 
#--------------------------------------------------------------
f4 = None 
list_f4 = []
dt_string = datetime.now().strftime('%y%m%d-%H%M%S')

def delete_initial_rows(inf4, i=0):
    file = open(inf4, 'r', encoding='ISO-8859-1')
    lines = file.readlines()
    file.close()

    f4_newname = f'{dt_string}_f4_{i}.txt'
    f = open('input/' + f4_newname, 'w')
    f.writelines(lines[10:])
    f.close()
    return f4_newname

# Si son varios archivos de F4s 
if num_f4_files > 1: 
    for i in range(num_f4_files): 
        f4_name = delete_initial_rows(f'input/{f4_input_name}_{i}.txt', i)
        f4_aux = pd.read_csv(f'input/{f4_name}', sep=';', dtype='object', error_bad_lines=False)
        list_f4.append(f4_aux)
    f4 = pd.concat(list_f4, axis=0)
else:
    f4_name = delete_initial_rows(f'input/{f4_input_name}.txt')
    f4 = pd.read_csv('input/'+ f4_name, sep=';', dtype='object', error_bad_lines=False)

# Limpieza del f4 

shape_v1 = f4.shape  # Obtiene la dimensión del dataframe

# Elimina las filas dos o menos valores no nulos
vacias = f4[f4.isnull().sum(axis=1) >= (shape_v1[1]-2)]
vacias.to_csv(f'output/{dt_string}-f4-vacias.csv', sep=';')
f4.dropna(thresh=2, inplace=True)
n_vacias = shape_v1[0]-f4.shape[0]

# Identifica las entradas en Nro. Red. Inventario nulas y reemplazar por texto
f4['Nro. Red. Inventario'].fillna('N/A', inplace=True)

# Identifica los registros desplazados
rd = f4[(~f4['Nro. Red. Inventario'].str.isdigit()) | (f4['Nro. Red. Inventario'].str.startswith('1'))]
rd.to_csv(f'output/{dt_string}-f4-desplazadas.csv', sep=';')

# Ajusta los valores desplazados
indice = rd.index
faux = f4.copy()

for i in indice:
    f4.loc[i-1, 'Destino':'Total Precio Costo'] = faux.loc[i,'Nro. Red. Inventario':'Linea'].values

f4[f4['Nro. Red. Inventario'].isna()].to_csv(f'output/{dt_string}-f4-error.csv', sep=';', index=False)
f4['Nro. Red. Inventario'].fillna('N/A', inplace=True)
f4 = f4[(f4['Nro. Red. Inventario'].str.isdigit()) & (~f4['Nro. Red. Inventario'].str.startswith('1'))]

# Extraer el F11
f4['F11'] = f4.Destino.str.extract('(\d{8,})')  # Extrae el valor F11
f4.to_csv(f'output/{dt_string}-f4-output.csv', sep=';', index=False)
num_fonces = f4['F11'].notna().sum()

print(f'Se encontró: \n {n_vacias} filas con dos o menos valores no nulos \n {indice.shape[0]}  registros desplazados \n{num_fonces} registros con valores de F11')
