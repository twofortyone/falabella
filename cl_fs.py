import pandas as pd
from datetime import datetime
from cl_cleaning import CleaningText as ct 

# Variables 
dt_string = datetime.now().strftime('%y%m%d-%H%M%S')

# Functions 
def delete_initial_rows(text_file, fname):
    file = open(text_file, 'r', encoding='ISO-8859-1')
    lines = file.readlines()
    file.close()
    f = open('input/' + fname, 'w')
    f.writelines(lines[10:])
    f.close()
    return fname

def clean_f3():
    # Verificar para cada archivo 
    f3_input_name = '210712_f3' # Prefijo del nombre del archivo 
    #-------------------------------------------------------------
    f3_name = delete_initial_rows(f'input/{f3_input_name}.txt', f'{dt_string}_f3.txt')
    f3 = pd.read_csv(f'input/{f3_name}', sep=';', dtype='object', error_bad_lines=False)
    a = f3.shape[0]
    # Obtener filas vacias 
    vacias = f3[f3['Fecha Reserva'].isna()] 
    f3 = f3[f3['Fecha Reserva'].notna()] 
    
    desplazadas = f3[f3.isna().sum(axis=1) >= f3.shape[1]-2]
    indice_des = desplazadas.index
    # Actualiza los valores de F11 desplazados
    for i in indice_des:
        f3.loc[i-1,'NC Proveedor':'Folio F11'] = f3.loc[i,'Nro Devolucion':'Fecha Reserva'].values

    print(f'{vacias.shape[0]} registros de F11s vacios')
    print(f'{desplazadas.shape[0]} registros de F11s desplazados')
    res1 = f3[~f3.index.isin(indice_des)]
    
    #print(list(indice_des.isin(f3.index)))
    res = f3[f3.isna().sum(axis=1) < f3.shape[1]-2]
    #print(res1[~res1.index.isin(res.index)])
    #print(res.shape)
    print(f'Se actualizaron {a-res.shape[0]} registros')
    vacias.to_csv(f'output/{dt_string}-f3-vacias.csv', sep=';', index=False)
    desplazadas.to_csv(f'output/{dt_string}-f3-desplazadas.csv', sep=';', index=False)
    res.to_csv(f'output/{dt_string}-f3-output.csv', sep=';', index=False)

def clean_f4():
    # Verificar para cada archivo 
    num_f4_files = 2
    f4_input_name = '210712_f4' # Prefijo del nombre del archivo 
    #--------------------------------------------------------------
    f4 = None 
    list_f4 = []
    # Si son varios archivos de F4s 
    if num_f4_files > 1: 
        for i in range(num_f4_files): 
            f4_name = delete_initial_rows(f'input/{f4_input_name}_{i}.txt', f'{dt_string}_f4_{i}.txt')
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
    rd.to_csv(f'output/{dt_string}-f4-desplazadas.csv', sep=';', index=False)

    # Ajusta los valores desplazados
    indice = rd.index
    faux = f4.copy()

    for i in indice:
        f4.loc[i-1, 'Destino':'Total Precio Costo'] = faux.loc[i,'Nro. Red. Inventario':'Linea'].values

    f4[f4['Nro. Red. Inventario'].isna()].to_csv(f'output/{dt_string}-f4-error.csv', sep=';', index=False)
    f4['Nro. Red. Inventario'].fillna('N/A', inplace=True)
    f4 = f4[(f4['Nro. Red. Inventario'].str.isdigit()) & (~f4['Nro. Red. Inventario'].str.startswith('1'))]

    # Extraer el F11
    f4['F11'] = f4.Destino.str.extract('([1]\d{7,})')  # Extrae el valor F11
    f4.to_csv(f'output/{dt_string}-f4-output.csv', sep=';', index=False)
    num_fonces = f4['F11'].notna().sum()

    print(f'Se encontró: \n {n_vacias} filas con dos o menos valores no nulos \n {indice.shape[0]}  registros desplazados \n{num_fonces} registros con valores de F11')

def clean_f5():
    # Verificar para cada archivo 
    num_f5_files = 4
    f5_input_name = '210709_f5' # Prefijo del nombre del archivo 
    #--------------------------------------------------------------
    f5 = None 
    list_f5 = []

    # Si son varios archivos de F4s 
    if num_f5_files > 1: 
        for i in range(num_f5_files): 
            f5_aux = pd.read_csv(f'input/{f5_input_name}_{i}.csv', sep=';', dtype='object', error_bad_lines=False)
            list_f5.append(f5_aux)
        f5 = pd.concat(list_f5, axis=0)
    else:
        f5 = pd.read_csv(f'input/{f5_input_name}.csv', sep=';', dtype='object', error_bad_lines=False)
    print('Guardado con éxito!')
    f5.to_csv(f'output/{dt_string}-f5-output.csv', sep=';', index=False)

def clean_kpi():
    kpi = pd.read_excel('input/210712_kpi.xlsx', dtype='object')
    kpi = ct.norm_header(kpi)
    kpi.rename(columns={'index': 'ind'}, inplace=True)
    kpi.reset_index(inplace=True)

    kpi['aaaa paletiza'] = kpi['fecha_paletiza'].str.extract('(\d{4})')
    kpi['entrada'] = kpi.entrada.str.extract('(\d+)', expand=False)

    du = kpi[kpi.duplicated(subset=['entrada'],keep=False)]
    td = du[du['aaaa paletiza'] !='2021']
    kpi.drop(index=td['index'].values, inplace=True)
    kpi.drop_duplicates(subset=['entrada'], inplace=True) # Agradado el 1 de junio para correcci{on }

    du.to_csv(f'output/{dt_string}-du.csv', sep=';', decimal=',', index=False)
    kpi.to_csv(f'output/{dt_string}-kpi.csv', sep=';', decimal=',', index=False)
    print('Guardado con éxito!')


# Menú de opciones 
print('-------  Plantillas SRX')
print('1. Limpiar plantilla F3')
print('2. Limpiar plantilla F4')
print('3. Limpiar plantilla F5')
print('4. Limpiar plantilla KPI')
print('5. Limpiar F3, F4, F5 y KPI')
selection_num = input('Seleccione una plantilla de F (1-5):')

if selection_num =='1': 
    clean_f3()
elif selection_num =='2':
    clean_f4()
elif selection_num =='3':
    clean_f5()
elif selection_num =='4':
    clean_kpi()
elif selection_num == '5':
    clean_f3()
    clean_f4()
    clean_f5()
    clean_kpi()
else:
    print('Por favor seleccione una opción valida')