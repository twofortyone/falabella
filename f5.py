import pandas as pd
from datetime import datetime
import numpy as np

# Verificar para cada archivo 
num_f5_files = 6
f5_input_name = 'f5' # Prefijo del nombre del archivo 
#--------------------------------------------------------------
f5 = None 
list_f5 = []
dt_string = datetime.now().strftime('%y%m%d-%H%M%S')

# Si son varios archivos de F4s 
if num_f5_files > 1: 
    for i in range(num_f5_files): 
        f5_aux = pd.read_csv(f'input/{f5_input_name}_{i}.csv', sep=';', dtype='object', error_bad_lines=False)
        list_f5.append(f5_aux)
    f5 = pd.concat(list_f5, axis=0)
else:
    f5 = pd.read_csv(f'input/{f5_input_name}.csv', sep=';', dtype='object', error_bad_lines=False)

f5.to_csv(f'output/{dt_string}-f5-output.csv', sep=';', index=False)
