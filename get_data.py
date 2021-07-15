# Librerías
import pandas as pd
from datetime import datetime
from cl_cleaning import CleaningText as ct 
from tqdm import tqdm

dt_string = datetime.now().strftime('%y%m%d-%H%M%S')

# Configuraciones
pd.set_option('float_format', '{:,.2f}'.format)

print('Generar datos')
print('1. Cierres de F11s CD auditoria')
print('2. Cierres de F11s CD 2021')
print('3. Cierres de F11s Tienda')
print('4. Cierres de NCs')
data_select = input('Seleccione una opción (1-4):')

# Cargar data
f3 = pd.read_csv(f'input/init_data/210713-111810-f3-output.csv', sep=';', dtype='object')
f4 = pd.read_csv(f'input/init_data/210713-111810-f4-output.csv', sep=';', dtype='object')
f5 = pd.read_csv(f'input/init_data/210713-111810-f5-output.csv', sep=';', dtype='object')
kpi = pd.read_csv(f'input/init_data/210713-111810-kpi.csv', sep=';', dtype='object')
refact = pd.read_csv(f'input/init_data/210616-refact.csv', sep=';', dtype='object')

# Declaración de columnas requeridas 
f3_colsreq = ['nro_devolucion', 'fecha_reserva', 'fecha_envio', 'fecha_anulacion', 'fecha_confirmacion', 'upc', 'sku', 'linea', 'descripcion6', 'cantidad', 'folio_f11', 'folio_f12']
f4_colsreq = ['nro_red_inventario', 'estado','fecha_creacion', 'destino', 'linea','upc', 'cantidad','f11']
f5_colsreq = ['transfer', 'estado', 'fe_reserva', 'fe_envo', 'fe_recep','local_envo', 'local_recep', 'tipo_de_f5','sku', 'upc', 'cant_pickeada', 'cant_recibida', 'motivo_discrepancia']
kpi_colsreq = ['index', 'tip0_trabajo', 'entrada','fecha_paletiza', 'aaaa_paletiza']
refac_colsreq = ['medio_pago','cod#aut', '4_ult', 'f12cod', 'orden_de_compra','cedula', 'valor_boleta','fecha_devolucion', 'confirmacion_facturacion', 'confirmacion_tesoreria']
cf11_20_colsreq  = ['nfolio','f12', 'prd_upc', 'qproducto', 'xobservacion', 'total_costo_promedio', 'estado_actual', 'status_nuevo', 'f3nuevo', 'f4_nuevo', 'nuevo_f11', 'f5'] # Para cd 2020 
cf11_21_colsreq  = ['f11','f12', 'prd_upc', 'qproducto', 'xobservacion', 'costo_total', 'estado_actual', 'status_final', 'f3', 'f4', 'f5'] # Para cd 2021 
cf11_tienda_colsreq = ['nfolio','upc', 'estado', 'producto', 'qproducto', 'total_costo_promedio', 'f', 'f3', 'f4', 'motivo']
cnc_colsreq = ['cod_aut_nc', 'local_trx', 'terminal', 'local_ant', 'upc', 'ct', 'cantidad_trx_actual', 'tipo_nc', 'f3', 'f4','f5', 'f11', 'esmc', 'tipmc']

# Inicializar estructuras según tipo análisis
lista =[f3, f4, f5, kpi, refact]
names = ['f3', 'f4', 'f5', 'kpi','refact']
dfs_colsreq = [f3_colsreq , f4_colsreq , f5_colsreq , kpi_colsreq , refac_colsreq]

# Columnas con datos númericos 
# Números de Fs, upcs, sku
f3_fnum = ['nro_devolucion','upc', 'sku','folio_f11', 'folio_f12']
f4_fnum = ['nro_red_inventario', 'upc', 'f11']
f5_fnum = ['transfer', 'sku', 'upc']
kpi_fnum = ['entrada']
refact_fnum = ['cod#aut', '4_ult', 'f12cod', 'orden_de_compra','cedula']
cf11_20_fnum = ['nfolio','f12', 'prd_upc', 'f3nuevo', 'f4_nuevo', 'nuevo_f11', 'f5']
cf11_21_fnum = ['f11','f12', 'prd_upc', 'f3', 'f4', 'f5']
cf11_tienda_fnum = ['nfolio', 'upc', 'f', 'f3', 'f4']
cnc_fnum = ['cod_aut_nc','local_trx', 'terminal', 'local_ant', 'upc', 'f3', 'f4', 'f5', 'f11']

# Costos y cantidades 
f3_num = ['cantidad']
f4_num = ['cantidad']
f5_num = ['cant_pickeada', 'cant_recibida']
cf11_20_num = [ 'qproducto', 'total_costo_promedio'] # Para cd 2020 
cf11_21_num = [ 'qproducto', 'costo_total'] 
cf11_tienda_num = [ 'qproducto', 'total_costo_promedio'] 
cnc_num = [ 'ct', 'cantidad_trx_actual'] 

lista_fnum= [f3_fnum, f4_fnum, f5_fnum, kpi_fnum, refact_fnum]
lista_num= [f3_num, f4_num, f5_num,'kpi', 'refac']

# Texto 
f3_text = ['linea', 'descripcion6']
f4_text = ['estado','destino', 'linea']
f5_text = ['estado', 'tipo_de_f5', 'motivo_discrepancia']
kpi_text = ['tip0_trabajo']
refact_text = ['medio_pago','confirmacion_facturacion', 'confirmacion_tesoreria']
cf11_20_text = ['xobservacion','estado_actual', 'status_nuevo']
cf11_21_text = ['xobservacion','estado', 'status_final']
cf11_tienda_text = ['motivo', 'estado']
cnc_text = ['esmc', 'tipmc']
lista_text = [f3_text, f4_text, f5_text, kpi_text, refact_text]

# Data aggregation 
if data_select=='1': # CF11s CD 2020 
    cf11_20= pd.read_csv(f'input/init_data/210712_cierres_f11_20.csv', sep=';',dtype='object')
    lista.append(cf11_20)
    names.append('cf11_cd_20')
    dfs_colsreq.append(cf11_20_colsreq)
    lista_fnum.append(cf11_20_fnum)
    lista_num.append(cf11_20_num)
    lista_text.append(cf11_20_text)

elif data_select=='2': # CF11s 2021 
    cf11_21 = pd.read_csv(f'input/init_data/210629_cierre_21.csv', sep=';',dtype='object')
    lista.append(cf11_21)
    names.append('cf11_cd_21')
    dfs_colsreq.append(cf11_21_colsreq)
    lista_fnum.append(cf11_21_fnum)
    lista_num.append(cf11_21_num)
    lista_text.append(cf11_21_text)

elif data_select =='3': # CF11s Tienda 2020 
    cf11_tienda = pd.read_csv(f'input/init_data/210713-081523-cf11_tienda_20.csv', sep=';',dtype='object')
    lista.append(cf11_tienda)
    names.append('cf11_tienda_20')
    dfs_colsreq.append(cf11_tienda_colsreq)
    lista_fnum.append(cf11_tienda_fnum)
    lista_num.append(cf11_tienda_num)
    lista_text.append(cf11_tienda_text)

elif data_select == '4': # Cierres NCs 
    cierres_nc = pd.read_csv(f'input/init_data/210709-151757-nc.csv', sep=';',dtype='object')
    lista.append(cierres_nc)
    names.append('cierres_nc')
    dfs_colsreq.append(cnc_colsreq)
    lista_fnum.append(cnc_fnum)
    lista_num.append(cnc_num)
    lista_text.append(cnc_text)

else: 
    print('Seleccione una opción correcta (1-4)')

# Normailzar headers
print('Normalizando encabezados')
for item in tqdm(lista): 
    ct.norm_header(item)

# Limpiar texto
print('Limpiando texto en columnas')
for i, item in enumerate(tqdm(lista_text)):
    lista[i].loc[:, item] = lista[i].loc[:, item].apply(ct.clean_str)

# Eliminar columnas no requeridas
def drop_except(df, cols):
    df.drop(df.columns.difference(cols), axis=1, inplace=True)
    return df 

print('Eliminando columnas no requeridas')
for i in tqdm(range(len(lista))): 
    drop_except(lista[i],dfs_colsreq[i])

# Convertir a número fs 
print('Convirtiendo a número parte 1')
for i, item in enumerate(tqdm(lista_fnum)):
    lista[i].loc[:, item] = lista[i].loc[:, item].apply(ct.clean_fnum)

# Convertir a número cantidades y costos 
print('Convirtiendo a número parte 2')
for i, item in enumerate(tqdm(lista_num)):
    if (i!=3)&(i!=4): 
        lista[i].loc[:, item] = lista[i].loc[:, item].apply(ct.clean_num)

# Eliminar filas duplicados 
lista[0].drop_duplicates(['nro_devolucion', 'upc'], inplace= True)
lista[1].drop_duplicates(['nro_red_inventario', 'upc'], inplace=True)
lista[2].drop_duplicates(['transfer','upc'], inplace=True)
lista[3].drop_duplicates(['entrada'], inplace=True)
lista[4].drop_duplicates(['f12cod', 'orden_de_compra'], inplace=True)

# Eliminar registros con #s de F nulos 
lista[0] = f3[f3.nro_devolucion.notna()]
lista[1] = f4[f4.nro_red_inventario.notna()]
lista[2] = f5[f5.transfer.notna()]
lista[3] = kpi[kpi.entrada.notna()]
lista[4] = refact[refact.f12cod.notna()]

# Guardar archivos 
print('Guardando archivos')
for i in tqdm(range(len(lista))):
    lista[i].to_csv(f'output/{dt_string}-{names[i]}.csv', sep=';', index=False, encoding='utf-8') 