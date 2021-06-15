# Librerías
import pandas as pd
from datetime import datetime
import numpy as np
from cl_cleaning import CleaningText as ct 
from ica import InternalControlAnalysis
from report import Report 
from unidecode import unidecode


# Configuraciones
pd.set_option('float_format', '{:,.2f}'.format)

# Variables 
dt_string = datetime.now().strftime('%y%m%d-%H%M%S')
index_name = 'indice_bc11'
cost_column = 'total_costo_promedio'
status_column = 'status nuevo'

f3_name = '210609-061940-f3'
f4_name = '210608-221323-f4-output'
kpi_name = '210608-215240-kpi'
bc11_name = '210608_cierre_f11'

f12_col ='f12'
f4_col ='f4 nuevo'
f3_col = 'f3nuevo'
f11_col = 'folio'

f4 = pd.read_csv(f'input/{f4_name}.csv', sep=';', dtype='object')
f3 = pd.read_csv(f'input/{f3_name}.csv', sep=';', dtype='object')
kpi = pd.read_csv(f'input/{kpi_name}.csv', sep=';', dtype='object')
bc11 = pd.read_csv(f'input/{bc11_name}.csv', sep=';',dtype='object')

# Normalizar nombres de columnsa 
f4 = ct.normalizar_cols(f4)
f3 = ct.normalizar_cols(f3)
bc11 = ct.normalizar_cols(bc11)

kpi['fecha_paletiza'] = pd.to_datetime(kpi['fecha_paletiza'])

bc11.drop([  'xsubprod', 'mprecio_vta', 'total_mprecio', 'cestado_dt', 'dpactada_dt',
       'csucursal', 'crut_clt', 'xdv_rut', 'propietario', 'dpactada_hd',
       'xnombre_clt', 'xapellido_clt', 'xdirec_clt', 'xciudad_clt', 'ccomuna',
       'dcreacion', 'ddespacho', 'qimpreso', 'xobservacion2', 'borigen', 
       'name_org', 'prd_lvl_child', 'costo_promedio', 
       'dias', 'antigaoeedad', 'grupo', 'fecha_base_local', 'estado_actual',
       'comentarios', 'mc', 'ee(f11)', 'fecha cambio de estado', 'reporte a contabilidad', 
       'nc','tranf electro factura', 'pv', 'observacion f4', 'ro', 'fecha ro'], axis=1, inplace=True)

f4.drop([ 'local', 'desc. local', 'tipo red.inv', 'usuario creacion', 'fecha reserva',
       'usuario reserva', 'fecha envio', 'usuario envio', 'destino',
       'r.u.t destino', 'centro de costos', 'desc.centro e costo', 'linea',
        'subclase', 'descripcion subclase','nro. producto', 'precio vta', 
       'precio costo', 'total precio vta', 'total precio costo',], axis=1, inplace = True)

f3.drop([ 'nro guia', 'tipo producto', 'marca', 'subclase', 'descripcion.1', 'clase',
       'descripcion.2', 'sublinea', 'descripcion.3', 'linea', 'descripcion.4',
       'proveedor', 'rut proveedor', 'descripcion.5', 'estado', 'cant*costo', 'cant*costoprmd',
       'diferencia', 'cant*precio', 'tipo documento para dev',
       'usuario que confirma', 'nc proveedor'], axis=1, inplace=True )

bc11 = ct.limpiar_cols(bc11, [f11_col, f12_col, f3_col, f4_col, status_column])
f4 = ct.limpiar_cols(f4, ['nro. red. inventario','upc', 'cantidad'])

bc11 = ct.convertir_a_numero(bc11, [cost_column, 'qproducto']) # Convertir columnas de precio a dato numérico
bc11.prd_upc= bc11.prd_upc.str.split('.').str[0] # Limpiar la columna de upc 

f4 = ct.convertir_a_numero(f4, ['cantidad']) # Convertir columnas de precio a dato numérico
f3 = ct.convertir_a_numero(f3, ['cantidad']) # Convertir columnas de precio a dato numérico

# Obtener el año de la reserva, el envío y la recepción
# datecolsf4 = ['fecha creacion',  'fecha reserva', 'fecha envio']
# newdatecolf4 = ['aa creacion',  'aa reserva', 'aa envio']
# f4[newdatecolf4] = f4[datecolsf4].apply(lambda x: x.str.extract('(\d{2})', expand=False))
# TODO Pasar esto a limpieza F4 y lo de borrar lineas de txt 
colsf3 = ['fecha reserva', 'fecha envio', 'fecha anulacion','fecha confirmacion']
newcolsf3 = ['aaaa reserva', 'aaaa envio', 'aaaa anulacion','aaaa confirmacion']
f3[newcolsf3] = f3[colsf3].apply(lambda x: x.str.extract('(\d{4})', expand=False))

f4['aa creacion'] = f4['fecha creacion'].str.split('-').str[2]

# Generar indice en columna
bc11.reset_index(inplace=True)
bc11.rename(columns={'index': index_name}, inplace=True)
bc11[status_column] = bc11[status_column].apply(unidecode)

ica = InternalControlAnalysis(bc11, index_name, cost_column)

# Funciones 
def cierre_f4(bd, status):
    df1 = bd[bd[status_column]==status]
    df2 = ica.get_fnan( df1, f4_col, 'F4')
    df3 = ica.get_duplicates( df2, [f12_col,'prd_upc', 'qproducto'], 'F12 + UPC + Cantidad')
    ne = ica.get_notfound( df3, f4, [f4_col,'prd_upc'], ['nro. red. inventario','upc'], 'nro. red. inventario', 'F4|UPC|QTY')
    df4 = pd.merge(df3, f4, left_on=[f4_col,'prd_upc'], right_on=['nro. red. inventario','upc'])
    df5 = ica.get_equalvalue(df4, 'estado', 'Anulado', 'ANU', 'Registro anulado')
    df6 = ica.get_diffvalue(df5, 'aa creacion', '2021', 'NAA', 'Registro con año de creación diferente a 2021')
    df7 = ica.get_diffqty_pro(df6, 'qproducto', 'cantidad',f11_col, f4_col, 'La cantidad sumada de los F11s de un F4 es mayor que la cantidad del F4')
    iokf4 = df7[index_name].values
    ica.update_db(iokf4,'GCO', 'OKK')
    ica.update_db(iokf4,'Comentario GCO', 'Coincidencia exacta F4+UPC+QTY')


cierre_f4(bc11, 'cierre x f4 cobrado a terceros')
cierre_f4(bc11, 'f4 de merma')

def cierre_f420(bd, status):
    df1 = bd[bd[status_column]==status]
    df2 = ica.get_fnan( df1, f4_col, 'F4')
    df3 = ica.get_duplicates( df2, [f12_col,'prd_upc', 'qproducto'], 'F12 + UPC + Cantidad')
    ne = ica.get_notfound( df3, f4, [f4_col,'prd_upc'], ['nro. red. inventario','upc'], 'nro. red. inventario', 'F4|UPC|QTY')
    df4 = pd.merge(df3, f4, left_on=[f4_col,'prd_upc'], right_on=['nro. red. inventario','upc'])
    df5 = ica.get_equalvalue(df4, 'estado', 'Anulado', 'ANU', 'Registro anulado')
    df6 = ica.get_diffvalue(df5, 'aa creacion', '2020', 'NAA', 'Registro con año de creación diferente a 2021')
    df7 = ica.get_diffqty_pro(df6, 'qproducto', 'cantidad',f11_col, f4_col, 'La cantidad sumada de los F11s de un F4 es mayor que la cantidad del F4')
    iokf4 = df7[index_name].values
    ica.update_db(iokf4,'GCO', 'OKK')
    ica.update_db(iokf4,'Comentario GCO', 'Coincidencia exacta F4+UPC+QTY')

def cierre_f3(bd):
    df1 = bd[bd[status_column]=='cierre x f3 devuelto a proveedor']
    df2= ica.get_fnan( df1, f3_col, 'F3')
    df3 = ica.get_duplicates( df2,[f12_col,'prd_upc', 'qproducto'], 'F12 + UPC + Cantidad')
    ne = ica.get_notfound( df3, f3, [f3_col,'prd_upc'], ['nro devolucion','upc'], 'nro devolucion', 'F3|UPC|QTY')
    df4 = pd.merge(df3, f3, left_on=[f3_col,'prd_upc'], right_on=['nro devolucion','upc'])
    df5 = ica.get_equalvalue(df4, 'descripcion.6', 'Anulado', 'ANU', 'Registro anulado')
    df6 = ica.get_diffqty_pro(df5, 'qproducto', 'cantidad',f11_col, f3_col,'La cantidad sumada de los f11s de un f3 es mayor que la cantidad del f3')
    iokf3 = df6[index_name].values
    ica.update_db(iokf3,'GCO', 'OKK')
    ica.update_db(iokf3,'Comentario GCO', 'Coincidencia exacta F3+UPC+QTY')
    df7 = df6[df6['descripcion.6']=='Confirmado']
    df8= ica.get_diffvalue(df7, 'aaaa anulacion', '2021', 'NAA', 'Registro con año de confirmación diferente a 2021')

def cierre_kpi(bd, status, yyyy, commenty): 
    df1 = bd[bd[status_column]==status]
    df2= ica.get_fnan_cols(df1, [f12_col,f11_col], 'KPID')
    df3 = ica.get_duplicates( df2, [f12_col,'prd_upc', 'qproducto'], 'F12 + UPC + Cantidad')

    index_ne_kpi_di = ica.get_notfound( df3, kpi, [f12_col], ['entrada'], 'entrada', '(F12|F11)+UPC+QTY')
    index_ne_kpi_di2 = ica.get_notfound( bd.loc[index_ne_kpi_di], kpi, [f11_col], ['entrada'], 'entrada', '(F12|F11)+UPC+QTY')

    pgdim1 = pd.merge(df3, kpi, left_on=[f12_col], right_on=['entrada'])
    pgdim2 = pd.merge(df3.loc[index_ne_kpi_di], kpi, left_on=[f11_col], right_on=['entrada'])
    lpgdi = [pgdim1, pgdim2]
    pgdim = pd.concat(lpgdi, axis=0)
    pgdimdyear = '' 
    if yyyy == '2021': 
        pgdimdyear = ica.get_diffvalue(pgdim, 'aaaa paletiza', yyyy, 'NAA',commenty)
    else:
        pgdimdyear = ica.get_menorvalue(pgdim, 'fecha_paletiza', '2021-01-20', 'NAA', commenty)
    iokkpid = pgdimdyear[index_name].values
    ica.update_db(iokkpid,'GCO', 'OKK')
    ica.update_db(iokkpid,'Comentario GCO', 'Coincidencia exacta (F12|F11)+UPC+QTY')

cierre_f420(bc11, 'f4 de merma-2020')
cierre_f3(bc11)
cierre_kpi(bc11, 'cierre x producto guardado despues de inventario', '2021', 'Recibido antes de 2021')
cierre_kpi(bc11, 'cierre x producto guardado antes de inventario', '0000', 'Recibido después del 20 de enero de 2021')

# # Tareas finales 
bc11 = ica.get_db()
reporte = Report('base cierre f11s')
print('\n ----------------- Base cierres de F11s antes de inventario ----------------- ')
res = bc11.groupby([status_column,'GCO']).agg({cost_column:['sum', 'size']}).sort_values(by=[status_column,(cost_column,'sum')], ascending=False)
print(res)# Presenta todos los estados 


# bc11.to_csv(f'output/{dt_string}-bc11.csv', sep=';', index=False, encoding='utf-8') # Guarda el archivo 
# bdcia = bc11.merge(f4, how='left',  left_on=[f4_col,'prd_upc'], right_on=['nro. red. inventario','upc'])
# #bdcia2 = bdcia.merge(f3, how='left', left_on=[f3_col,'prd_upc'], right_on=['nro devolucion','upc'])
# bdcia.to_csv(f'output/{dt_string}-bc11cia.csv', sep=';', index=False) 

