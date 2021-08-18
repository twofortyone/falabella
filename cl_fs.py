import io
import pandas as pd
from datetime import datetime
from cl_cleaning import CleaningText as ct 
from get_data import GetData
from get_data import menu as mgd

# Variables 
dt_string = datetime.now().strftime('%y%m%d-%H%M')
config = open('input/cl_fs_config.txt', 'r', encoding='ISO-8859-1')
clines = [line.strip() for line in config.readlines()]
config.close()

# Functions 
def delete_initial_rows(text_file):
    file = open(text_file, 'r', encoding='ISO-8859-1')
    flines = file.readlines()[10:]
    file.close()
    return flines

def clean_f3(f3_input_name):
    f3_lines = delete_initial_rows(f'input/planillas/{f3_input_name}.txt')
    f3 = pd.read_csv(io.StringIO("\n".join(f3_lines)), sep=';', dtype='object', error_bad_lines=False)
    # TODO eliminar error_bad_lines cuando accesos directos a DB 
    a = f3.shape[0]
    # Obtener filas vacias 
    vacias = f3[f3['Fecha Reserva'].isna()] 
    f3 = f3[f3['Fecha Reserva'].notna()] 
    
    desplazadas = f3[f3.isna().sum(axis=1) >= f3.shape[1]-2]
    indice_des = desplazadas.index
    # Actualiza los valores de F11 desplazados
    for i in indice_des:
        f3.loc[i-1,'NC Proveedor':'Folio F11'] = f3.loc[i,'Nro Devolucion':'Fecha Reserva'].values

    print(f'   {vacias.shape[0]} registros de F11s vacios')
    print(f'   {desplazadas.shape[0]} registros de F11s desplazados')
    res1 = f3[~f3.index.isin(indice_des)]
    
    #print(list(indice_des.isin(f3.index)))
    res = f3[f3.isna().sum(axis=1) < f3.shape[1]-2]
    #print(res1[~res1.index.isin(res.index)])
    #print(res.shape)
    print(f'   Se actualizaron {a-res.shape[0]} registros')
    vacias.to_csv(f'output/planillas/{dt_string}-f3-vacias.csv', sep=';', index=False)
    desplazadas.to_csv(f'output/planillas/{dt_string}-f3-desplazadas.csv', sep=';', index=False)
    f3_path = f'output/planillas/{dt_string}-f3-output.csv'
    res.to_csv(f3_path, sep=';', index=False)
    print('-- Planilla F3 guardada con éxito!')
    print(f'   dir: {f3_path}')
    return f3_path

def clean_f4(f4_input_name, num_f4_files):
    f4 = None 
    list_f4 = []
    # Si son varios archivos de F4s 
    if num_f4_files > 1: 
        for i in range(num_f4_files): 
            f4_lines = delete_initial_rows(f'input/planillas/{f4_input_name}_{i}.txt')
            f4_aux = pd.read_csv(io.StringIO("\n".join(f4_lines)), sep=';', dtype='object', error_bad_lines=False)
            list_f4.append(f4_aux)
        f4 = pd.concat(list_f4, axis=0)
    else:
        f4_lines = delete_initial_rows(f'input/planillas/{f4_input_name}.txt')
        f4 = pd.read_csv(io.StringIO("\n".join(f4_lines)), sep=';', dtype='object', error_bad_lines=False)

    # Limpieza del f4 
    shape_v1 = f4.shape  # Obtiene la dimensión del dataframe

    # Elimina las filas dos o menos valores no nulos
    vacias = f4[f4.isnull().sum(axis=1) >= (shape_v1[1]-2)]
    f4.dropna(thresh=2, inplace=True)
    n_vacias = shape_v1[0]-f4.shape[0]

    # Identifica las entradas en Nro. Red. Inventario nulas y reemplazar por texto
    f4['Nro. Red. Inventario'].fillna('N/A', inplace=True)

    # Identifica los registros desplazados
    rd = f4[(~f4['Nro. Red. Inventario'].str.isdigit()) | (f4['Nro. Red. Inventario'].str.startswith('1'))]

    # Ajusta los valores desplazados
    indice = rd.index
    faux = f4.copy()

    for i in indice:
        f4.loc[i-1, 'Destino':'Total Precio Costo'] = faux.loc[i,'Nro. Red. Inventario':'Linea'].values

    f4[f4['Nro. Red. Inventario'].isna()].to_csv(f'output/planillas/{dt_string}-f4-error.csv', sep=';', index=False)
    f4['Nro. Red. Inventario'].fillna('N/A', inplace=True)
    f4 = f4[(f4['Nro. Red. Inventario'].str.isdigit()) & (~f4['Nro. Red. Inventario'].str.startswith('1'))]

    # Extraer el F11
    f4['F11'] = f4.Destino.str.extract('([1]\d{7,})')  # Extrae el valor F11
    num_fonces = f4['F11'].notna().sum()
    print(f'   {n_vacias} filas con dos o menos valores no nulos \n   {indice.shape[0]}  registros desplazados \n   {num_fonces} registros con valores de F11')
    
    # Guardar los archivos 
    vacias.to_csv(f'output/planillas/{dt_string}-f4-vacias.csv', sep=';')
    rd.to_csv(f'output/planillas/{dt_string}-f4-desplazadas.csv', sep=';', index=False)
    f4_path = f'output/planillas/{dt_string}-f4-output.csv'
    f4.to_csv(f4_path, sep=';', index=False)
    print('-- Planilla F4 guardada con éxito!')
    print(f'   dir: {f4_path}')
    return f4_path

def clean_f5(f5_input_name, num_f5_files):
    f5 = None 
    list_f5 = []

    # Si son varios archivos de F4s 
    if num_f5_files > 1: 
        for i in range(num_f5_files): 
            f5_aux = pd.read_csv(f'input/planillas/{f5_input_name}_{i}.csv', sep=';', dtype='object', error_bad_lines=False)
            list_f5.append(f5_aux)
        f5 = pd.concat(list_f5, axis=0)
    else:
        f5 = pd.read_csv(f'input/planillas/{f5_input_name}.csv', sep=';', dtype='object', error_bad_lines=False)
    
    # Guardar archivos
    f5_path = f'output/planillas/{dt_string}-f5-output.csv'
    f5.to_csv(f5_path, sep=';', index=False)
    print('-- Planilla F5 guardada con éxito!')
    print(f'   dir: {f5_path}')
    return f5_path
    

def clean_kpi(kpiname):
    kpi = pd.read_excel(f'input/planillas/{kpiname}.xlsx', dtype='object')
    kpi = ct.norm_header(kpi)
    kpi.rename(columns={'index': 'ind'}, inplace=True)
    kpi.reset_index(inplace=True)

    kpi['aaaa paletiza'] = kpi['fecha_paletiza'].str.extract('(\d{4})')
    kpi['entrada'] = kpi.entrada.str.extract('(\d+)', expand=False)

    du = kpi[kpi.duplicated(subset=['entrada'],keep=False)]
    td = du[du['aaaa paletiza'] !='2021']
    kpi.drop(index=td['index'].values, inplace=True)
    kpi.drop_duplicates(subset=['entrada'], inplace=True) # Agradado el 1 de junio para correcci{on }

    # Guardar archivos 
    du.to_csv(f'output/planillas/{dt_string}-kpi-du.csv', sep=';', decimal=',', index=False)
    kpi_path = f'output/planillas/{dt_string}-kpi-output.csv'
    kpi.to_csv(kpi_path, sep=';', decimal=',', index=False)
    print('-- Planilla kpi guardada con éxito!')
    print(f'   dir: {kpi_path}')
    return kpi_path

def excel_to_csv(filename, sheetname):
    dbexcel = pd.read_excel(f'input/bases/{filename}.xlsx', sheet_name=sheetname, dtype='object')
    db_path = f'output/bases/{dt_string}-{filename}-output.csv'
    dbexcel.to_csv(db_path, sep=';', index=False, encoding='utf-8') 
    print('-- DB guardada con éxito!')
    print(f'   dir: {db_path}')
    return db_path


def get_data(f3, f4, f5, kpi, refact, db):
    gd = GetData()
    selection_var = mgd()
    gd.load_data(f3, f4, f5, kpi, refact, db)
    gd.run_gd(selection_var)

# Menú de opciones 
print('-------  Plantillas SRX')
print('1. Limpiar plantilla F3')
print('2. Limpiar plantilla F4')
print('3. Limpiar plantilla F5')
print('4. Limpiar plantilla KPI')
print('5. Convertir DB excel a csv')
print('6. Obtener F3, F4, F5, KPI y DB')
print('-------')
selection_num = input('Seleccione una opción (1-6):')

if selection_num =='1': 
    clean_f3(clines[0])
elif selection_num =='2':
    clean_f4(clines[1], int(clines[2]))
elif selection_num =='3':
    clean_f5(clines[3], int(clines[4]))
elif selection_num =='4':
    clean_kpi(clines[5])
elif selection_num== '5':
    excel_to_csv(clines[6], clines[7])
elif selection_num == '6':
    f3_path = clean_f3(clines[0])
    f4_path = clean_f4(clines[1], int(clines[2]))
    f5_path = clean_f5(clines[3], int(clines[4]))
    kpi_path = clean_kpi(clines[5])
    refact_path = clines[8]
    db_path = excel_to_csv(clines[6], clines[7])

    sp = input('Desea procesar la data? (y/n)')
    if sp =='y':
        get_data(f3_path, f4_path, f5_path, kpi_path, refact_path, db_path)
    else: 
        print('-- La data ha sido guardada sin procesar en output/planillas/ y output/bases/')
else:
    print('Por favor seleccione una opción valida')