# Librerías
import pandas as pd
from datetime import datetime
import numpy as np
from cleaning import CleaningText as ct 
from ica import InternalControlAnalysis
from report import Report 
from unidecode import unidecode


# Configuraciones
pd.set_option('float_format', '{:,.2f}'.format)

# Variables 
dt_string = datetime.now().strftime('%y%m%d-%H%M%S')
index_name = 'indice_bc11'
cost_column = 'total_costo_promedio'
status_column = 'estatus final'

f4 = pd.read_csv('input/cierresf11s/210603_f4.csv', sep=';', dtype='object')
f3 = pd.read_csv('input/cierresf11s/210601-195542-f3v1-output.csv', sep=';', dtype='object')
kpi = pd.read_csv('input/cierresf11s/210601-153009-kpi.csv', sep=';', dtype='object')
bc11 = pd.read_csv('input/cierresf11s/bc1106.csv', sep=';', dtype='object')

# Normalizar nombres de columnsa 
f4 = ct.normalizar_cols(f4)
f3 = ct.normalizar_cols(f3)
bc11 = ct.normalizar_cols(bc11)

kpi['fecha_paletiza'] = pd.to_datetime(kpi['fecha_paletiza'])

bc11.drop(['mprecio_vta', 'total_mprecio', 'cestado_dt','dpactada_dt','crut_clt', 
        'xdv_rut', 'propietario','dpactada_hd', 'xnombre_clt', 'xapellido_clt', 'xdirec_clt',
        'xciudad_clt', 'ccomuna', 'dcreacion', 'ddespacho', 'qimpreso', 'xobservacion', 'xobservacion2', 'cestado', 
        'xestado', 'cservicio','xservicio', 'borigen', 'suc', 'name_org', 'prd_lvl_child',
        'costo_promedio','dias', 'antigaoeedad','grupo', 'fecha_base_local', 'estado_actual', 
        'comentarios f4','transportadora', 'nc', 'tranf electro factura', 'pv', 'observacion f4',
        'comentarios','mc', 'ee(f11)','ro', 'fecha ro', 'nuevo f11', 'csucursal'], axis=1, inplace=True)

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

bc11 = ct.limpiar_cols(bc11, ['nfolio', 'f12', 'f3', 'f4', 'estatus final'])
f4 = ct.limpiar_cols(f4, ['nro. red. inventario','upc', 'cantidad'])

bc11 = ct.convertir_a_numero(bc11, [cost_column]) # Convertir columnas de precio a dato numérico
bc11.prd_upc= bc11.prd_upc.str.split('.').str[0] # Limpiar la columna de upc 

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

##################################################################
#----------------- Cierre x F4 Cobrado a terceros-----------------

def cierre_f4(bd, status):
    df1 = bd[bd[status_column]==status]
    df2 = ica.get_fnan( df1, 'f4', 'F4')
    df3 = ica.get_duplicates( df2, ['f12','prd_upc', 'qproducto'], 'F4')
    ne = ica.get_notfound( df3, f4, ['f4','prd_upc'], ['nro. red. inventario','upc'], 'nro. red. inventario', 'F4')
    df4 = pd.merge(df3, f4, left_on=['f4','prd_upc'], right_on=['nro. red. inventario','upc'])
    df5 = ica.get_equalvalue(df4, 'estado', 'Anulado', 'F4', 'ANU')
    df6 = ica.get_diffvalue(df5, 'aa creacion', '2021', 'F4', 'NAA')
    df7 = ica.get_diffqty(df6, 'qproducto', 'cantidad','F4')
    iokf4 = df7[index_name].values
    bc11 = ica.get_db()
    bc11.loc[iokf4, 'GCO'] = 'OKK'

cierre_f4(bc11, 'cierre x f4 cobrado a terceros')
cierre_f4(bc11, 'f4 de merma')
cierre_f4(bc11, 'f4 en proceso de clasificacion')

##################################################################
#----------------- Cierre x F3 Devuelto a proveedor -----------------

cdp = bc11[bc11[status_column]=='cierre x f3 devuelto a proveedor']
ncdp = cdp.shape[0]
ccdp= cdp[[cost_column]].sum()

cdp2= ica.get_fnan( cdp, 'f3', 'F3')
cdp3= ica.get_duplicates( cdp2,['f12','prd_upc', 'qproducto'], 'F3')
ne_f3= ica.get_notfound( cdp3, f3, ['f3','prd_upc'], ['nro devolucion','upc'], 'nro devolucion', 'F3')
bc11f3 = pd.merge(cdp3, f3, left_on=['f3','prd_upc'], right_on=['nro devolucion','upc'])
banu_f3 = ica.get_equalvalue(bc11f3, 'descripcion.6', 'Anulado', 'F3', 'ANU')
bf3dqty = ica.get_diffqty(banu_f3, 'qproducto', 'cantidad','F3')

iokf3 = bf3dqty[index_name].values
bc11 = ica.get_db()
bc11.loc[iokf3, 'GCO'] = 'OKK'

banuconfir = bf3dqty[bf3dqty['descripcion.6']=='Confirmado']
banuyear_f3= ica.get_diffvalue(banuconfir, 'aaaa anulacion', '2021', 'F3', 'NAA')

# bc11 = ica.get_db()
# bc11.to_csv(f'output/{dt_string}-bc11f3.csv', sep=';', decimal=',', index=False) # Guarda el archivo 

#######################################################################################
#----------------- 'Cierre x producto guardado despues de Inventario' -----------------

pgdi = bc11[bc11[status_column]=='cierre x producto guardado despues de inventario']
npgdi = pgdi.shape[0]
cpgdi= pgdi[[cost_column]].sum()

gdi2= ica.get_fnan_cols( pgdi, ['f12','nfolio'], 'KPID')
gdi3= ica.get_duplicates( gdi2, ['f12','prd_upc', 'qproducto'], 'KPID')
index_ne_kpi_di = ica.get_notfound( gdi3, kpi, ['f12'], ['entrada'], 'entrada', 'KPIDF12')
index_ne_kpi_di2 = ica.get_notfound( bc11.loc[index_ne_kpi_di], kpi, ['nfolio'], ['entrada'], 'entrada', 'KPID')

pgdim1 = pd.merge(gdi3, kpi, left_on=['f12'], right_on=['entrada'])
pgdim2 = pd.merge(gdi3.loc[index_ne_kpi_di], kpi, left_on=['nfolio'], right_on=['entrada'])
lpgdi = [pgdim1, pgdim2]
pgdim = pd.concat(lpgdi, axis=0)

pgdimdyear = ica.get_diffvalue(pgdim, 'aaaa paletiza', '2021', 'KPID', 'NAA')

iokkpid = pgdimdyear[index_name].values
bc11 = ica.get_db()
bc11.loc[iokkpid, 'GCO'] = 'OKK'

######################################################################################
#----------------- 'Cierre x producto guardado antes de Inventario' -----------------
pgai = bc11[bc11[status_column]=='cierre x producto guardado antes de inventario']
npgai = pgai.shape[0]
cpgai= pgai[[cost_column]].sum()

gai2 = ica.get_fnan_cols( pgai, ['f12','nfolio'], 'KPIA')
gai3= ica.get_duplicates( gai2, [ 'f12','prd_upc', 'qproducto'], 'KPIA')

index_ne_kpi_ai = ica.get_notfound( gai3, kpi, ['f12'], ['entrada'], 'entrada', 'KPIAF12')
index_ne_kpi_ai2= ica.get_notfound( bc11.loc[index_ne_kpi_ai], kpi, ['nfolio'], ['entrada'], 'entrada', 'KPIA')

pgaim1 = pd.merge(gai3, kpi, left_on=['f12'], right_on=['entrada'])
pgaim2 = pd.merge(gai3.loc[index_ne_kpi_ai], kpi, left_on=['nfolio'], right_on=['entrada'])
lpgai = [pgaim1, pgaim2]
pgaim = pd.concat(lpgai, axis=0)

pgaimdyear = ica.get_menorvalue(pgaim, 'fecha_paletiza', '2021-01-20', 'KPIA', 'NAA')


iokkpia = pgaimdyear[index_name].values
bc11 = ica.get_db()
bc11.loc[iokkpia, 'GCO'] = 'OKK'


# # Tareas finales 
bc11.to_csv(f'output/{dt_string}-bc11.csv', sep=';', decimal=',', index=False, encoding='utf-8') # Guarda el archivo 
bdcia = bc11.merge(f4, how='left',  left_on=['f4','prd_upc'], right_on=['nro. red. inventario','upc'])
bdcia2 = bdcia.merge(f3, how='left', left_on=['f3','prd_upc'], right_on=['nro devolucion','upc'])
bdcia2.to_csv(f'output/{dt_string}-bc11cia.csv', sep=';', decimal=',', index=False) 

# --------------------------------------------------------------------------------Reporte 
reporte = Report('base cierre f11s')
print('\n ----------------- Base cierres de F11s antes de inventario ----------------- ')

print('\n ## Resumen de información según estado')
print(bc11[[status_column, cost_column]].groupby(status_column).sum().sort_values(by=cost_column, ascending=False))

print('----------------- Cierre x F4 Cobrado a terceros-----------------')
print(bc11[bc11[status_column]=='cierre x f4 cobrado a terceros'][[status_column, cost_column,'GCO']].groupby([status_column,'GCO']).agg(['sum', 'size']).sort_values(by=(cost_column,'sum'), ascending=False))

print('----------------- Cierre x F4 Merma -----------------')
print(bc11[bc11[status_column]=='f4 de merma'][[status_column, cost_column,'GCO']].groupby([status_column,'GCO']).agg(['sum', 'size']).sort_values(by=(cost_column,'sum'), ascending=False))

print('----------------- Cierre x F4 en proceso de clasificación -----------------')
print(bc11[bc11[status_column]=='f4 en proceso de clasificacion'][[status_column, cost_column,'GCO']].groupby([status_column,'GCO']).agg(['sum', 'size']).sort_values(by=(cost_column,'sum'), ascending=False))

print('----------------- Cierre x F3 Devuelto a proveedor -----------------')
print(bc11[bc11[status_column]=='cierre x f3 devuelto a proveedor'][[status_column, cost_column,'GCO']].groupby([status_column,'GCO']).agg(['sum', 'size']).sort_values(by=(cost_column,'sum'), ascending=False))

print('----------------- Cierre x producto guardado despues de Inventario -----------------')
print(bc11[bc11[status_column]=='cierre x producto guardado despues de inventario'][[status_column, cost_column,'GCO']].groupby([status_column,'GCO']).agg(['sum', 'size']).sort_values(by=(cost_column,'sum'), ascending=False))

print('----------------- Cierre x producto guardado antes de Inventario -----------------')
print(bc11[bc11[status_column]=='cierre x producto guardado antes de inventario'][[status_column, cost_column,'GCO']].groupby([status_column,'GCO']).agg(['sum', 'size']).sort_values(by=(cost_column,'sum'), ascending=False))

#----------------------------------------------------------------------------------------------------- Fin 
# nr=[nnr, b6f5.shape[0], cnr]
# md = [nmd, b6f52.shape[0], cmd]
# ncc = [nncc, b6f53.shape[0], cncc]
# total = [nfnan + ndu + nne + nnr + nmd + nncc, nb6c, cfnan + cdu + cne + cnr + cmd + cncc, ]
# summaryres = b6[[cost_column, 'CIF5', 'estado_2']].groupby(['estado_2', 'CIF5']).agg(['sum', 'size']).sort_values(by=(cost_column,'sum'), ascending=False)
# reporte.print_analysis(comp='F5', comments='para estado cerrado', total= total, nan=nan, du=du, ne=nex, nr=nr, md=md, dc=ncc, summary=summaryres)