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

f4 = pd.read_csv('input/cierresf11s/210601-111903-f4-output.csv', sep=';', dtype='object')
f3 = pd.read_csv('input/cierresf11s/210601-195542-f3v1-output.csv', sep=';', dtype='object')
kpi = pd.read_csv('input/cierresf11s/210601-153009-kpi.csv', sep=';', dtype='object')
bc11 = pd.read_csv('input/cierresf11s/0602_base.csv', sep=';', dtype='object')

# Normalizar nombres de columnsa 
f4 = ct.normalizar_cols(f4)
f3 = ct.normalizar_cols(f3)
bc11 = ct.normalizar_cols(bc11)

#bc11['estatus final']= unidecode(list(bc11['estatus final']))

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

ica = InternalControlAnalysis(bc11, index_name, cost_column)

##################################################################
#----------------- Cierre x F4 Cobrado a terceros-----------------

cct = bc11[bc11[status_column]=='Cierre x F4 Cobrado a terceros']
ncct = cct.shape[0]
ccct= cct[[cost_column]].sum()

cct2, nfnan, cfnan = ica.get_fnan( cct, 'f4', 'F4')
cct3, ndu, cdu = ica.get_duplicates( cct2, ['f12','prd_upc', 'qproducto'], 'F4')
ne, nne, cne = ica.get_notfound( cct3, f4, ['f4','prd_upc'], ['nro. red. inventario','upc'], 'nro. red. inventario', 'F4')
bc11f4 = pd.merge(cct3, f4, left_on=['f4','prd_upc'], right_on=['nro. red. inventario','upc'])
b4anu, nanu, canu = ica.get_equalvalue(bc11f4, 'estado', 'Anulado', 'F4', 'ANU')
b4dyear, ndy, cdy = ica.get_diffvalue(b4anu, 'aa creacion', '2021', 'F4', 'NAA')
b4dty, ndq, cdq = ica.get_diffqty(b4dyear, 'qproducto', 'cantidad','F4')

iokf4 = b4dty[index_name].values
bc11 = ica.get_db()
bc11.loc[iokf4, 'CIF4'] = 'OKK'
bc11.loc[iokf4, 'CIA'] = 'OKK'

nan = [nfnan, ncct, cfnan]
du = [ndu, cct2.shape[0], cdu]
nex = [nne, cct3.shape[0], cne]
anu = [nanu, bc11f4.shape[0], canu]
dy = [ndy, b4anu.shape[0], cdy]
dqty = [ndq, b4dyear.shape[0], cdq]

##################################################################
#----------------- Cierre x F4 Cobrado a terceros-----------------

x1 = bc11[bc11[status_column]=='F4 de merma']

x2, nfnan, cfnan = ica.get_fnan( x1, 'f4', 'F4M')
x3, ndu, cdu = ica.get_duplicates( x2, ['f12','prd_upc', 'qproducto'], 'F4M')
ica.get_notfound( x3, f4, ['f4','prd_upc'], ['nro. red. inventario','upc'], 'nro. red. inventario', 'F4M')
xm = pd.merge(x3, f4, left_on=['f4','prd_upc'], right_on=['nro. red. inventario','upc'])
x4, nanu, canu = ica.get_equalvalue(xm, 'estado', 'Anulado', 'F4M', 'ANU')
x5, ndy, cdy = ica.get_diffvalue(x4, 'aa creacion', '2021', 'F4M', 'NAA')
x6, ndq, cdq = ica.get_diffqty(x5, 'qproducto', 'cantidad','F4M')

iokf4x = x6[index_name].values
bc11 = ica.get_db()
bc11.loc[iokf4x, 'CIF4M'] = 'OKK'
bc11.loc[iokf4x, 'CIA'] = 'OKK'

##################################################################
#----------------- Cierre x F4 Cobrado a terceros-----------------

y1 = bc11[bc11[status_column]=='F4 en proceso de clasificacion']

y2, nfnan, cfnan = ica.get_fnan( y1, 'f4', 'F4PC')
y3, ndu, cdu = ica.get_duplicates( y2, ['f12','prd_upc', 'qproducto'], 'F4PC')
ica.get_notfound( y3, f4, ['f4','prd_upc'], ['nro. red. inventario','upc'], 'nro. red. inventario', 'F4PC')
ym = pd.merge(y3, f4, left_on=['f4','prd_upc'], right_on=['nro. red. inventario','upc'])
y4, nanu, canu = ica.get_equalvalue(ym, 'estado', 'Anulado', 'F4PC', 'ANU')
y5, ndy, cdy = ica.get_diffvalue(y4, 'aa creacion', '2021', 'F4PC', 'NAA')
y6, ndq, cdq = ica.get_diffqty(y5, 'qproducto', 'cantidad','F4PC')

iokf4y6 = y6[index_name].values
bc11 = ica.get_db()
bc11.loc[iokf4y6, 'CIF4PC'] = 'OKK'
bc11.loc[iokf4y6, 'CIA'] = 'OKK'

##################################################################
#----------------- Cierre x F3 Devuelto a proveedor -----------------

cdp = bc11[bc11[status_column]=='Cierre x F3 Devuelto a Proveedor']
ncdp = cdp.shape[0]
ccdp= cdp[[cost_column]].sum()

cdp2, nfnan_f3, cfnan_f3 = ica.get_fnan( cdp, 'f3', 'F3')
cdp3, ndu_f3, cdu_f3 = ica.get_duplicates( cdp2,['f12','prd_upc', 'qproducto'], 'F3')
ne_f3, nne_f3, cne_f3 = ica.get_notfound( cdp3, f3, ['f3','prd_upc'], ['nro devolucion','upc'], 'nro devolucion', 'F3')

bc11 = ica.get_db()
bc11.loc[ne_f3].to_csv(f'output/{dt_string}-bf3ne.csv', sep=';', decimal=',', index=False) 

bc11f3 = pd.merge(cdp3, f3, left_on=['f3','prd_upc'], right_on=['nro devolucion','upc'])
banu_f3, nanu_f3, canu_f3 = ica.get_equalvalue(bc11f3, 'descripcion.6', 'Anulado', 'F3', 'ANU')
bf3dqty, ndq_f3, cdq_f3 = ica.get_diffqty(banu_f3, 'qproducto', 'cantidad','F3')

iokf3 = bf3dqty[index_name].values
bc11 = ica.get_db()
bc11.loc[iokf3, 'CIF3'] = 'OKK'
bc11.loc[iokf3, 'CIA'] = 'OKK'

banuconfir = bf3dqty[bf3dqty['descripcion.6']=='Confirmado']
banuyear_f3, ndy_f3, cdy_f3 = ica.get_diffvalue(banuconfir, 'aaaa anulacion', '2021', 'F3', 'NAA')

nan_f3 = [nfnan_f3, ncdp, cfnan_f3]
du_f3 = [ndu_f3, cdp2.shape[0], cdu_f3]
nex_f3 = [nne_f3, cdp3.shape[0], cne_f3]
anu_f3 = [nanu_f3, bc11f3.shape[0], canu_f3]
dqty_f3 = [ndq_f3, banu_f3.shape[0], cdq_f3]
dy_f3 = [ndy_f3, banuconfir.shape[0], cdy_f3 ]

#######################################################################################
#----------------- 'Cierre x producto guardado despues de Inventario' -----------------

pgdi = bc11[bc11[status_column]=='Cierre x producto guardado despues de Inventario']
npgdi = pgdi.shape[0]
cpgdi= pgdi[[cost_column]].sum()

gdi2, nfnan_kpi_di, cfnan_kpi_di = ica.get_fnan( pgdi, 'f12', 'KPID')
gdi3, ndu_kpi_di, cdu_kpi_di = ica.get_duplicates( gdi2, ['f12','prd_upc', 'qproducto'], 'KPID')
index_ne_kpi_di, nne_kpi_di, cne_kpi_di = ica.get_notfound( gdi3, kpi, ['f12'], ['entrada'], 'entrada', 'KPIDF12')
index_ne_kpi_di2, nne_kpi_di2, cne_kpi_di2 = ica.get_notfound( bc11.loc[index_ne_kpi_di], kpi, ['nfolio'], ['entrada'], 'entrada', 'KPID')

pgdim1 = pd.merge(gdi3, kpi, left_on=['f12'], right_on=['entrada'])
pgdim2 = pd.merge(gdi3.loc[index_ne_kpi_di], kpi, left_on=['nfolio'], right_on=['entrada'])
lpgdi = [pgdim1, pgdim2]
pgdim = pd.concat(lpgdi, axis=0)

pgdimdyear, ndy_kpid, cdy_kpid = ica.get_diffvalue(pgdim, 'aaaa paletiza', '2021', 'KPID', 'NAA')

iokkpid = pgdimdyear[index_name].values
bc11 = ica.get_db()
bc11.loc[iokkpid, 'CIKPID'] = 'OKK'
bc11.loc[iokkpid, 'CIA'] = 'OKK'

nan_kpi_di = [nfnan_kpi_di, npgdi, cfnan_kpi_di]
du_kpi_di = [ndu_kpi_di, gdi2.shape[0], cdu_kpi_di]
nex_kpi_di = [nne_kpi_di, gdi3.shape[0], cne_kpi_di]
nex_kpi_di2 = [nne_kpi_di2, gdi3.shape[0], cne_kpi_di2]
dy_kpid = [ndy_kpid, pgdim.shape[0], cdy_kpid ]
######################################################################################
#----------------- 'Cierre x producto guardado antes de Inventario' -----------------
pgai = bc11[bc11[status_column]=='Cierre x producto guardado antes de Inventario']
npgai = pgai.shape[0]
cpgai= pgai[[cost_column]].sum()

gai2, nfnan_kpi_ai, cfnan_kpi_ai = ica.get_fnan( pgai, 'f12', 'KPIA')
gai3, ndu_kpi_ai, cdu_kpi_ai = ica.get_duplicates( gai2, [ 'f12','prd_upc', 'qproducto'], 'KPIA')

index_ne_kpi_ai, nne_kpi_ai, cne_kpi_ai = ica.get_notfound( gai3, kpi, ['f12'], ['entrada'], 'entrada', 'KPIAF12')
index_ne_kpi_ai2, nne_kpi_ai2, cne_kpi_ai2 = ica.get_notfound( bc11.loc[index_ne_kpi_ai], kpi, ['nfolio'], ['entrada'], 'entrada', 'KPIA')

pgaim1 = pd.merge(gai3, kpi, left_on=['f12'], right_on=['entrada'])
pgaim2 = pd.merge(gai3.loc[index_ne_kpi_ai], kpi, left_on=['nfolio'], right_on=['entrada'])
lpgai = [pgaim1, pgaim2]
pgaim = pd.concat(lpgai, axis=0)

pgaimdyear, ndy_kpia, cdy_kpia = ica.get_equalvalue(pgaim, 'aaaa paletiza', '2021', 'KPIA', 'NAA')

iokkpia = pgaimdyear[index_name].values
bc11 = ica.get_db()
bc11.loc[iokkpia, 'CIKPIA'] = 'OKK'
bc11.loc[iokkpia, 'CIA'] = 'OKK'

nan_kpi_ai = [nfnan_kpi_ai, npgai, cfnan_kpi_ai]
du_kpi_ai = [ndu_kpi_ai, gai2.shape[0], cdu_kpi_ai]

nex_kpi_ai = [nne_kpi_ai, gai3.shape[0], cne_kpi_ai]
nex_kpi_ai2 = [nne_kpi_ai2, '0', cne_kpi_ai2]
dy_kpia = [ndy_kpia, pgaim.shape[0], cdy_kpia ]

# # Tareas finales 
bc11.to_csv(f'output/{dt_string}-bc11.csv', sep=';', decimal=',', index=False) # Guarda el archivo 
bdcia = bc11.merge(f4, how='left',  left_on=['f4','prd_upc'], right_on=['nro. red. inventario','upc'])
bdcia2 = bdcia.merge(f3, how='left', left_on=['f3','prd_upc'], right_on=['nro devolucion','upc'])
bdcia2.to_csv(f'output/{dt_string}-bc11cia.csv', sep=';', decimal=',', index=False) 

# --------------------------------------------------------------------------------Reporte 
reporte = Report('base cierre f11s')
print('\n ----------------- Base cierres de F11s antes de inventario ----------------- ')
print('\n ## Resumen de información según estado')
print(bc11[[status_column, cost_column]].groupby(status_column).sum().sort_values(by=cost_column, ascending=False))

print('----------------- Cierre x F4 Cobrado a terceros-----------------')
print(f'nan {nan}')
print(f'dup {du}')
print(f'nex {nex}')
print(f'anulados {anu}')
print(f'dif year {dy}')
print(f'dif qty {dqty}')
print(f'{cfnan+cdu+cne+canu+cdy +cdq :,.2f}')
print(bc11[bc11[status_column]=='Cierre x F4 Cobrado a terceros'][[status_column, cost_column,'CIF4']].groupby([status_column,'CIF4']).agg(['sum', 'size']).sort_values(by=(cost_column,'sum'), ascending=False))
print(bc11[bc11[status_column]=='F4 de merma'][[status_column, cost_column,'CIF4M']].groupby([status_column,'CIF4M']).agg(['sum', 'size']).sort_values(by=(cost_column,'sum'), ascending=False))
print(bc11[bc11[status_column]=='F4 en proceso de clasificacion'][[status_column, cost_column,'CIF4PC']].groupby([status_column,'CIF4PC']).agg(['sum', 'size']).sort_values(by=(cost_column,'sum'), ascending=False))

print('----------------- Cierre x F3 Devuelto a proveedor -----------------')
print(f'nan {nan_f3}')
print(f'dup {du_f3}')
print(f'nex {nex_f3}')
print(f'anu {anu_f3}')
print(f'dif qty {dqty_f3}')
print(f'dif year {dy_f3}')
print(f'{cfnan_f3+cdu_f3+ cne_f3 + canu_f3 +cdy_f3 + cdq_f3:,.2f}')
print(bc11[bc11[status_column]=='Cierre x F3 Devuelto a Proveedor'][[status_column, cost_column,'CIF3']].groupby([status_column,'CIF3']).agg(['sum', 'size']).sort_values(by=(cost_column,'sum'), ascending=False))

print('----------------- Cierre x producto guardado despues de Inventario -----------------')
print(f'nan {nan_kpi_di}')
print(f'dup {du_kpi_di}')
print(f'nex f12 {nex_kpi_di}')
print(f'nex f11 {nex_kpi_di2}')
print(f'dif year {dy_kpid}')
print(f'{cfnan_kpi_di+ cdu_kpi_di + cne_kpi_di2 + cdy_kpid:,.2f}')
print(bc11[bc11[status_column]=='Cierre x producto guardado despues de Inventario'][[status_column, cost_column,'CIKPID']].groupby([status_column,'CIKPID']).agg(['sum', 'size']).sort_values(by=(cost_column,'sum'), ascending=False))


print('----------------- Cierre x producto guardado antes de Inventario -----------------')
print(f'nan {nan_kpi_ai}')
print(f'dup {du_kpi_ai}')
print(f'nex f12 {nex_kpi_ai}')
print(f'nex f11 {nex_kpi_ai2}')
print(f'dif year {dy_kpia}')
print(f'{cfnan_kpi_ai+ cdu_kpi_ai + cne_kpi_ai2 + cdy_kpia:,.2f}')
print(bc11[bc11[status_column]=='Cierre x producto guardado antes de Inventario'][[status_column, cost_column,'CIKPIA']].groupby([status_column,'CIKPIA']).agg(['sum', 'size']).sort_values(by=(cost_column,'sum'), ascending=False))

#----------------------------------------------------------------------------------------------------- Fin 
# nr=[nnr, b6f5.shape[0], cnr]
# md = [nmd, b6f52.shape[0], cmd]
# ncc = [nncc, b6f53.shape[0], cncc]
# total = [nfnan + ndu + nne + nnr + nmd + nncc, nb6c, cfnan + cdu + cne + cnr + cmd + cncc, ]
# summaryres = b6[[cost_column, 'CIF5', 'estado_2']].groupby(['estado_2', 'CIF5']).agg(['sum', 'size']).sort_values(by=(cost_column,'sum'), ascending=False)
# reporte.print_analysis(comp='F5', comments='para estado cerrado', total= total, nan=nan, du=du, ne=nex, nr=nr, md=md, dc=ncc, summary=summaryres)