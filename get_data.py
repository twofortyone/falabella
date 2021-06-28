# Librerías
import pandas as pd
from datetime import datetime
from cl_cleaning import CleaningText as ct 
from unidecode import unidecode

dt_string = datetime.now().strftime('%y%m%d-%H%M%S')

# Configuraciones
pd.set_option('float_format', '{:,.2f}'.format)

# Cargar data
f4 = pd.read_csv(f'input/init_data/210625-101207-f4-output.csv', sep=';', dtype='object')
f3 = pd.read_csv(f'input/init_data/210625-100948-f3.csv', sep=';', dtype='object')
kpi = pd.read_csv(f'input/init_data/210623-090420-kpi.csv', sep=';', dtype='object')
refact = pd.read_csv(f'input/init_data/210616-refact.csv', sep=';', dtype='object')
f5 = pd.read_csv(f'input/init_data/210625-101509-f5-output.csv', sep=';', dtype='object')
bc11 = pd.read_csv(f'input/init_data/210623_cierre_f11.csv', sep=';',dtype='object')
bc11_tienda = pd.read_csv(f'input/init_data/210623_cf11_tienda.csv', sep=';',dtype='object')
cierres_nc = pd.read_csv(f'input/init_data/210625_nc.csv', sep=';',dtype='object')

# Normalizar nombres de columnas  
lista =[f3, f4, f5, kpi, refact, bc11, bc11_tienda, cierres_nc]
names = ['f3', 'f4', 'f5', 'kpi','refact', 'cierre_f11', 'cf11_tienda', 'cierre_nc']

for item in lista: 
    ct.norm_header(item)


dfs = [bc11, f3, f4, f5, kpi, bc11_tienda, cierres_nc]
bc11_cols = ['nfolio','f12', 'prd_upc', 'qproducto', 'xobservacion', 'total_costo_promedio', 'estado_actual', 'status_nuevo', 'f3nuevo', 'f4_nuevo', 'nuevo_f11', 'f5']
f3_cols = ['nro_devolucion', 'fecha_reserva', 'fecha_envio', 'fecha_anulacion', 'fecha_confirmacion', 'upc', 'sku', 'linea', 'descripcion6', 'cantidad', 'folio_f11', 'folio_f12']
f4_cols = ['nro_red_inventario', 'estado','fecha_creacion', 'destino', 'linea','upc', 'cantidad','f11']
f5_cols = ['transfer', 'estado', 'fe_reserva', 'fe_envo', 'fe_recep','local_envo', 'local_recep', 'tipo_de_f5','sku', 'upc', 'cant_pickeada', 'cant_recibida', 'motivo_discrepancia']
kpi_cols = ['index', 'tip0_trabajo', 'entrada','fecha_paletiza', 'aaaa_paletiza']
bc11_tienda_cols = ['nfolio','prd_upc', 'qproducto', 'total_costo_promedio', 'f', 'motivo']
cnc_cols = ['cod_aut_nc', 'local_trx', 'terminal', 'local_ant', 'upc', 'ct', 'cantidad_trx_actual', 'tipo_nc', 'f3', 'f4','f5', 'f11', 'esmc', 'tipmc']

dfs_cols = [bc11_cols, f3_cols, f4_cols, f5_cols, kpi_cols, bc11_tienda_cols, cnc_cols]

def drop_except(df, cols):
    df.drop(df.columns.difference(cols), axis=1, inplace=True)
    return df 

for i in range(len(dfs)): 
    drop_except(dfs[i],dfs_cols[i])


f4.drop_duplicates(['nro_red_inventario', 'upc'], inplace=True)
f3.drop_duplicates(['nro_devolucion', 'upc'], inplace= True)
f5.drop_duplicates(['transfer','upc'], inplace=True)
refact.drop_duplicates(['f12cod', 'orden_de_compra'], inplace=True)
kpi.drop_duplicates(['entrada'])

# Guardar archivos 
for i in range(len(lista)):
    lista[i].to_csv(f'output/{dt_string}-{names[i]}.csv', sep=';', index=False, encoding='utf-8') 