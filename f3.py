import pandas as pd
from datetime import datetime
import numpy as np

# Verificar para cada archivo 
num_f4_files = 2
f3_input_name = '210608_f3_0' # Prefijo del nombre del archivo 
#--------------------------------------------------------------
dt_string = datetime.now().strftime('%y%m%d-%H%M%S')

def delete_initial_rows(inf3, i=0):
    file = open(inf3, 'r', encoding='ISO-8859-1')
    lines = file.readlines()
    file.close()

    f3_newname = f'{dt_string}_f3_{i}.txt'
    f = open('input/' + f3_newname, 'w')
    f.writelines(lines[10:])
    f.close()
    return f3_newname

f3_name = delete_initial_rows(f'input/{f3_input_name}.txt')
f3 = pd.read_csv(f'input/{f3_name}', sep=';', dtype='object', error_bad_lines=False)
 
fv1 = f3[f3['Fecha Reserva'].notna()] 
rega = fv1.shape[0]
indice = fv1.loc[fv1['Nro Devolucion'].isnull(),:].index

# Recorre Fecha reserva para actualizar los valores de F11 desplazados 
for i in indice:
    fv1.loc[i-1,'Folio F11'] = fv1.loc[i,'Fecha Reserva']

print('Existen ' + str(indice.shape[0]) + ' registros de F11s desplazados')
fv1 = fv1[fv1['Nro Devolucion'].notnull()]

print('Se actualizaron ' + str(rega-fv1.shape[0]) + ' registros')

fv1.to_csv(f'output/{dt_string}-f3.csv', sep=';', index=False)
