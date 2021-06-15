import pandas as pd
from datetime import datetime
import numpy as np

# Verificar para cada archivo 
num_f4_files = 2
f4_input_name = '210608_f4' # Prefijo del nombre del archivo 
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
else:
    f4_name = delete_initial_rows(f'input/{f4_input_name}.txt')