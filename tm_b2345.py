import pandas as pd
from datetime import datetime
from cl_cleaning import CleaningText as ct 
from ica_raw import InternalControlAnalysis 

# Configuraciones 
pd.set_option('float_format', '{:,.2f}'.format) # Configura pandas para mostrar solo dos decimales 

# Importar bases de datos 
bd = pd.read_csv('input/base2345.csv', sep=';', dtype='object')
f4 = pd.read_csv('input/210513-015942-f4-output.csv', sep=';', dtype='object')
f3 = pd.read_csv('input/210513-204312-f3v1-output.csv', sep=';', dtype='object')
kpi = pd.read_csv('input/210528-153310-kpi.csv', sep=';', dtype='object')

# Variables
dt_string = datetime.now().strftime('%y%m%d-%H%M%S')
index_name = 'indice_b25'
cost_column = 'total'

# Normalizar nombres de columnas
bd = ct.normalizar_cols(bd)
f4 = ct.normalizar_cols(f4)
f3 = ct.normalizar_cols(f3)
kpi = ct.normalizar_cols(kpi)

# Convertir columnas de precio a dato numérico
bd = ct.convertir_a_numero(bd, ['precio_costo', 'total'])

# Generar indice en columna
bd.reset_index(inplace=True)
bd.rename(columns={'index': index_name}, inplace=True)

# Obtener el año de la reserva, el envío y la recepción
colsf4 = ['fecha creacion', 'fecha reserva', 'fecha envio']
newcolsf4 = ['aaaa creacion', 'aaaa reserva', 'aaaa envio']
f4[newcolsf4] = f4[colsf4].apply(lambda x: x.str.extract('(\d{4})', expand=False))

colsf3 = ['fecha reserva', 'fecha envio', 'fecha anulacion','fecha confirmacion']
newcolsf3 = ['aaaa reserva', 'aaaa envio', 'aaaa anulacion','aaaa confirmacion']
f3[newcolsf3] = f3[colsf3].apply(lambda x: x.str.extract('(\d{4})', expand=False))

kpi['aaaa paletiza'] = kpi['fecha_paletiza'].str.extract('(\d{4})')

# Extrae los valores númericos del campo entrada del kpi 
kpi['entrada'] = kpi.entrada.str.extract('(\d+)', expand=False)

# Análisis de F3
bd.loc[bd['estatus final']=='Recibido en CD', 'nef'] = 'Recibido en CD'
bd.loc[(bd['estatus final']=='Dado de baja por entregado a cliente -(Autoriza Gerencia de Control Operacional)') 
        | (bd['estatus final']=='Dado de baja por F12 entregado en tienda antes de inventario (Autoriza Gerencia de Control Operacional) ') 
        | (bd['estatus final']=='Dado de baja por error en la generación de la NC - Autoriza Gerencia de Control Operacional) ') 
        | (bd['estatus final']=='Cierre x F4 Cobrado a terceros'), 'nef'] = 'F4s'
bd.loc[bd['estatus final'] == 'Con F3 Proveedor ','nef'] = 'Con F3 Proveedor'

ica = InternalControlAnalysis(bd,index_name, cost_column)

dff3p = bd[(bd['estatus final'] == 'Con F3 Proveedor ') & (bd['tpificacion'] == 'CERRADO ')]

nf3p = dff3p.shape[0]
cf3p = dff3p[cost_column].sum()

dff3p2, nfnan, cfnan = ica.get_fnan( dff3p, 'f3', 'F3')
dff3p3, ndu, cdu = ica.get_duplicates( dff3p2, ['f3', 'upc'], 'F3')
ne, nne, cne = ica.get_notfound( dff3p3, f3, ['f3', 'upc'], ['nro devolucion', 'upc'], 'nro devolucion', 'F3')
bdf3cant = pd.merge(dff3p3, f3, left_on=['f3','upc'], right_on=['nro devolucion','upc']) # Unir b2345 con F3
bdf3cant2, ndc, cdc = ica.get_diffqty(bdf3cant, 'unidades', 'cantidad', 'F3')
bdf3cant3, nanu, canu = ica.get_canceledstatus(bdf3cant2, 'descripcion.6', 'F3') # TODO cambiar por get_equalvalue()

# TODO revisar porque no funciona 
bdf3conf = bdf3cant3[bdf3cant3['descripcion.6']=='Confirmado']
bdf3conf2, ndy, cdy = ica.get_diffyear(bdf3conf, 'aaaa anulacion', '2021', 'F3')

iokf3 = bdf3cant3[index_name].values
bd = ica.get_db()
bd.loc[iokf3, 'CIF3'] = 'OKK'
bd.loc[iokf3, 'CIA'] = 'OKK'

print('\n ----------------- Base 2345 ----------------- ')
print('\n ## Resumen de información según estatus final')
print(bd[['estatus final', cost_column]].groupby('estatus final').sum().sort_values(by=cost_column, ascending=False))
print('\n--------------------------------------------------------------------')
print('## Análisis con F3 proveedor')
print('--------------------------------------------------------------------')
print('# Resumen: ')
print(f'= {nfnan+ndu+nne+ndc+nanu+ndy} de {nf3p} registros con novedad, por un valor de: {cfnan+cdu+cne+cdc+canu+cdy:,.2f}')
print('# Detalle: ')
print(f'- {nfnan} de {nf3p} registros cerrados sin número de F3, por un valor de {cfnan:,.2f}')
print(f'- {ndu} de {dff3p2.shape[0]} registros duplicados, por un valor de {cdu:,.2f}')
print(f'- {nne} de {dff3p3.shape[0]} registros no se encontraron en la base de F3, por un valor de {cne:,.2f}')
print(f'- {ndc} de {bdf3cant.shape[0]} registros no coinciden con cantidad de la base de F3, por un valor de {cdc:,.2f}')
print(f'- {nanu} de {bdf3cant2.shape[0]} registros anulados en la base de F3, por un valor de {canu:,.2f}')
print(f'- {ndy} de {bdf3cant3.shape[0]} registros no coindicen con el año de confirmación 2021, por un valor de {cdy:,.2f}')
print(bd[[cost_column, 'CIF3', 'estatus final']].groupby(['estatus final', 'CIF3']).sum().sort_values(by=cost_column, ascending=False))
print('--------------------------------------------------------------------')

# Análisis de F4 
bdcf4 = bd[(bd['tpificacion'] == 'CERRADO ')]
dff4 = bdcf4[(bdcf4['estatus final']=='Dado de baja por entregado a cliente -(Autoriza Gerencia de Control Operacional)') 
        | (bdcf4['estatus final']=='Dado de baja por F12 entregado en tienda antes de inventario (Autoriza Gerencia de Control Operacional) ') 
        | (bdcf4['estatus final']=='Dado de baja por error en la generación de la NC - Autoriza Gerencia de Control Operacional) ') 
        | (bdcf4['estatus final']=='Cierre x F4 Cobrado a terceros')]

nf4 = dff4.shape[0]
cf4 = dff4[cost_column].sum()

dff42, nfnanf4, cfnanf4 = ica.get_fnan( dff4, 'f4', 'F4')
dff43, nduf4, cduf4 = ica.get_duplicates( dff42, ['f4', 'upc'], 'F4')
nef4, nnef4, cnef4 = ica.get_notfound( dff43, f4, ['f4', 'upc'], ['nro. red. inventario', 'upc'], 'nro. red. inventario', 'F4')
bdf4cant = pd.merge(dff43, f4, left_on=['f4','upc'], right_on=['nro. red. inventario','upc']) # Unir b2345 con F4
bdf4cant2, ndcf4, cdcf4 = ica.get_diffqty(bdf4cant, 'unidades', 'cantidad', 'F4')
bdf4cant3, nanuf4, canuf4 = ica.get_canceledstatus(bdf4cant2, 'estado', 'F4')
bdf4dy, ndyf4, cdyf4 = ica.get_diffyear(bdf4cant3, 'aaaa creacion', '2021', 'F4')

iokf4 = bdf4dy[index_name].values
bd = ica.get_db()
bd.loc[iokf4, 'CIF4'] = 'OKK'
bd.loc[iokf4, 'CIA'] = 'OKK'

print('## Análisis con F4 ')
print('--------------------------------------------------------------------')
print('# Resumen: ')
print(f'= {nfnanf4+nduf4+nnef4+ndcf4+nanuf4} de {nf4} registros con novedad, por un valor de: {cfnanf4+cduf4+cnef4+cdcf4+canu:,.2f}')
print('# Detalle: ')
print(f'- {nfnanf4} de {nf4} registros cerrados sin número de F4, por un valor de {cfnanf4:,.2f}')
print(f'- {nduf4} de {dff42.shape[0]} registros duplicados, por un valor de {cduf4:,.2f}')
print(f'- {nnef4} de {dff43.shape[0]} registros no se encontraron en la base de F4, por un valor de {cnef4:,.2f}')
print(f'- {ndcf4} de {bdf4cant.shape[0]} registros no coinciden con cantidad de la base de F4, por un valor de {cdcf4:,.2f}')
print(f'- {nanuf4} de {bdf4cant2.shape[0]} registros anulados en la base de F4, por un valor de {canuf4:,.2f}')
print(f'- {ndyf4} de {bdf4cant3.shape[0]} registros no coindicen con el año de confirmación 2021, por un valor de {cdyf4:,.2f}')
print(bd[[cost_column, 'CIF4', 'estatus final']].groupby(['estatus final', 'CIF4']).sum().sort_values(by=cost_column, ascending=False))
print('--------------------------------------------------------------------')

# Análisis de KPI
dfkpi = bd[(bd['estatus final']=='Recibido en CD') & (bd['tpificacion']=='CERRADO ')]
nkpi = dfkpi.shape[0]
ckpi = dfkpi[cost_column].sum()

dfkpi2, nfnankpi, cfnankpi = ica.get_fnan( dfkpi, 'nro_f12', 'KPI')
dfkpi3, ndukpi, cdukpi = ica.get_duplicates( dfkpi2, ['nro_f12', 'upc', 'unidades'], 'KPI')
nekpi, nnekpi, cnekpi = ica.get_notfound( dfkpi3, kpi, ['nro_f12'], ['entrada'], 'entrada', 'KPI')
bdkpi_year = pd.merge(dfkpi3, kpi, left_on=['nro_f12'], right_on=['entrada']) # Unir b2345 con KPI
bdkpi_year2, ndykpi, cdykpi = ica.get_diffyear(bdkpi_year, 'aaaa paletiza', '2021', 'KPI')

iok_kpi = bdkpi_year2[index_name].values
bd = ica.get_db()
bd.loc[iok_kpi, 'CIKPI'] = 'OKK'
bd.loc[iok_kpi, 'CIA'] = 'OKK'

print('## Análisis con KPI ')
print('--------------------------------------------------------------------')
print('# Resumen: ')
print(f'= {nfnankpi+ndukpi+nnekpi+ndykpi} de {nkpi} registros con novedad, por un valor de: {cfnankpi+cdukpi+cnekpi+cdykpi:,.2f}')
print('# Detalle: ')
print(f'- {nfnankpi} de {nkpi} registros cerrados sin número de F12, por un valor de {cfnankpi:,.2f}')
print(f'- {ndukpi} de {dfkpi2.shape[0]} registros duplicados, por un valor de {cdukpi:,.2f}')
print(f'- {nnekpi} de {dfkpi3.shape[0]} registros no se encontraron en la base de KPI, por un valor de {cnekpi:,.2f}')
print(f'- {ndykpi} de {bdkpi_year.shape[0]} registros no coindicen con el año paletiza 2021, por un valor de {cdykpi:,.2f}')
print('--------------------------------------------------------------------') 

tip = bd[bd['tpificacion']=='Cerrado']
gbd = bd[['nef', 'CIA',cost_column]].groupby(['nef','CIA']).agg({'sum','size'}).reset_index()
gbd.columns = gbd.columns.to_flat_index()
gbd.columns = ["_".join(a) for a in gbd.columns.to_flat_index()]
gbd.loc[gbd['nef_'] == 'Con F3 Proveedor', 'percentage'] = gbd.loc[gbd['nef_'] == 'Con F3 Proveedor', 'total_sum']/cf3p
gbd.loc[gbd['nef_'] == 'F4s', 'percentage']  = gbd.loc[gbd['nef_'] == 'F4s', 'total_sum'] / cf4
gbd.loc[gbd['nef_'] == 'Recibido en CD', 'percentage']  = gbd.loc[gbd['nef_'] == 'Recibido en CD', 'total_sum'] / ckpi
gbd.to_csv(f'output/{dt_string}-gbd.csv', sep=';', decimal=',', index=False)

# Tareas finales 
bd = ica.get_db()
bd.to_csv(f'output/{dt_string}-bd.csv', sep=';', decimal=',', index=False) # Guarda el archivo 
bdtotal = bd.merge(f3, how='left', left_on=['f3','upc'], right_on=['nro devolucion','upc'],indicator='F3_merge')
bdtotal = bdtotal.merge(f4, how='left', left_on=['f4','upc'], right_on=['nro. red. inventario','upc'], indicator='F4_merge')
bdtotal = bdtotal.merge(kpi, how='left', left_on='nro_f12', right_on='entrada', indicator='KPI_merge')
bdtotal.drop_duplicates(subset=['indice_b25'], inplace=True)
bdtotal.to_csv(f'output/{dt_string}-bdtotal.csv', sep=';', decimal=',', index=False)
#kpi.to_csv(f'output/{dt_string}-kpi.csv', sep=';', decimal=',', index=False)