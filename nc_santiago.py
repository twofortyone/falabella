import config.nc_webhook as data_nc
import pymsteams
from datetime import datetime
import pandas as pd 
from os import rename, sep
from textwrap import indent
import numpy as np 
import io
from datetime import timedelta, date, datetime
from os import listdir
from os.path import isfile, join

## -- Inicio files teams
dt_string = datetime.now().strftime('%y%m%d')

## -------------------------------------------------- TODO incorporar método 

def unir_ncs():
       path = 'input/ncs_aut/ncs/'
       files_names = [f for f in listdir(path) if isfile(join(path, f))]

       files_store = []
       
       for i in files_names: 
              file = open(f'input/ncs_aut/ncs/{i}', 'r', encoding='ISO-8859-1')
              nc_lines = file.readlines()
              file.close()
              files_store.append(pd.read_csv(io.StringIO("\n".join(nc_lines)), sep=';', dtype='object', error_bad_lines=False))

       nc_df = pd.concat(files_store)

       nc_df.to_csv('input/ncs_aut/ncs/output/nc_df.csv', index=False)

unir_ncs() # method call 

## --------------------------------------------------

# Files loading
ep = pd.read_excel('input/ncs_aut/211020_empleados_planta.xlsx', dtype='object') 
et = pd.read_excel('input/ncs_aut/211020_empleados_temporales.xlsx', dtype='object')
df_nc = pd.read_csv('input/ncs_aut/ncs/output/nc_df.csv', dtype='object')
df_v = pd.read_excel('input/ncs_aut/211104_ventas_oct_nov.xlsx', dtype='object') # Ventas 

# Convert numeric and date data 
df_nc.Qcantidad = pd.to_numeric(df_nc.Qcantidad)
df_nc.Cvendedor = pd.to_numeric(df_nc.Cvendedor)
df_nc.Mventa_nc = -pd.to_numeric(df_nc.Mventa_nc)
df_nc.Hora = pd.to_numeric(df_nc.Hora)

ep.Cod_Empleado  = pd.to_numeric(ep.Cod_Empleado)

df_nc.Dcompra_nvo = df_nc.Dcompra_nvo.str.replace('oct', '10')
df_nc.Dcompra_nvo = df_nc.Dcompra_nvo.str.replace('nov', '11')
df_nc.Dcompra_nvo = df_nc.Dcompra_nvo.str.replace('dic', '12')

df_nc.Dcompra_nvo = pd.to_datetime(df_nc.Dcompra_nvo, format='%d-%m-%Y') # TODO arreglar para español 

df_v['Día'] = pd.to_datetime(df_v['Día'], format='%d/%m/%Y')
df_v['Número de Vendedor (Cod.)'] = pd.to_numeric(df_v['Número de Vendedor (Cod.)'])
df_v['Venta en $'] = pd.to_numeric(df_v['Venta en $'])

# Filter columns 
df_nc = df_nc.loc[:,['Local_creacion', 'Desc_local', 'Dcompra_nvo', 'Nterminal_nvo',
       'Nsecuencia_nvo', 'Hora', 'Tipo_trx', 'Cautoriza', 'Estado', 'Usuario',
       'Cvendedor', 'Local_ant', 'Descr_local_ant', 'Dcompra_ant',
       'Nterminal_ant', 'Nsecuencia_ant', 'Cvendedor_ant', 'Cajero_apertura',
       'Linea', 'LiDescripcion','SKU', 'EAN', 'PDescripcion',
       'Cmarca', 'Tipo Producto','Nrutcomprador', 'Qcantidad', 'Mventa_nc','Xtipificacion']]

ep['nombre_completo'] = ep['Nombre'] + ' ' + ep['Apell_Paterno']

ep = ep.loc[:,[ 'Cod_Sucursal', 'Sucursal', 'Cod_Empleado', 'Num_Documento', 'FecInicioContrato', 'Cod_Cargo', 'Cargo', 'Cod_Depto',
       'Departamento','Nombre_Superior', 'nombre_completo']]

et = et.loc[:,['SUCURSAL', 'APELLIDOS Y NOMBRES', 'DOCUMENTO', 'FECHA INGRESO', 'CARGO', 'FECHA TERMINO', 'TDA_AREA']]

# Rename columns 
df_nc.rename(columns={'Mventa_nc':'Costo_NC-Empleado'}, inplace=True)
et.rename(columns={'SUCURSAL':'Sucursal', 'APELLIDOS Y NOMBRES':'nombre_completo', 'DOCUMENTO':'Num_Documento', 'FECHA INGRESO':'FecInicioContrato', 'CARGO':'Cargo', 'TDA_AREA':'Departamento'}, inplace=True)
df_v.rename(columns={'Número de Vendedor (Cod.)':'Cod_Empleado', 'Local':'Desc_local', 'Venta en $':'Costo_Venta-Empleado' }, inplace=True)

# Filters 
nc = df_nc.loc[df_nc['Tipo_trx']=='NC'] # Transaction type 
local_excluir = ['3000', '2000', '11', '99', '321', '143']
nc = nc.loc[~nc.Local_creacion.isin(local_excluir)] # Local number 
nc = nc.loc[nc['Cvendedor']!=47708] # Default seller 

nc.loc[nc['Desc_local']=='MARTINA COLINA', 'Desc_local'] = 'COLINA'
nc.loc[nc['Desc_local']=='MARTINA FONTANAR', 'Desc_local'] = 'FONTANAR'
nc.loc[nc['Desc_local']=='EXPO HAYUELOS', 'Desc_local'] = 'HAYUELOS'
# TODO df_v = df_v.loc[df_v['Día'] == '2021-09-20']

## -------------- Por mes 

initial_date = pd.to_datetime(date.today() - timedelta(days=30))
nc = nc.loc[nc.Dcompra_nvo >= initial_date]
df_v = df_v.loc[df_v['Día']>= initial_date]

# Merging files  
nme = nc.merge(ep, how='left', left_on=['Cvendedor'], right_on=['Cod_Empleado'])
nme2 = nme.loc[nme.Num_Documento.notna()]

# Grouping by 
gdfv = df_v.groupby(['Cod_Empleado', 'Desc_local']).agg({'Costo_Venta-Empleado':'sum'}).reset_index()

r1 = nme2.groupby(['Cod_Empleado', 'Num_Documento','nombre_completo', 'Desc_local','Cargo']).agg({'Cautoriza':'nunique',  'Qcantidad':'sum', 'Costo_NC-Empleado':'sum'}).reset_index()

r2 = r1.merge(gdfv, how='left', on=['Cod_Empleado', 'Desc_local'])
r2['Costo_NC-Tienda'] = r2.groupby(['Desc_local'])['Costo_NC-Empleado'].transform('sum')
r2['Costo_Venta-Tienda'] = r2.groupby(['Desc_local'])['Costo_Venta-Empleado'].transform('sum')

r2['NC/Venta-Empleado'] = r2['Costo_NC-Empleado']/r2['Costo_Venta-Empleado']
r2['Costo_NC-Empleado/Costo_NC-Tienda'] = r2['Costo_NC-Empleado']/r2['Costo_NC-Tienda']
r2['CVenta-Empleado/CVenta-Tienda'] = r2['Costo_Venta-Empleado']/r2['Costo_Venta-Tienda']

## -------------- Por día 

last_date = nc.Dcompra_nvo.max()
nc_dia = nc.loc[nc.Dcompra_nvo == last_date]
df_v_dia = df_v.loc[df_v['Día']== last_date]

# Merging files  
nme_dia = nc_dia.merge(ep, how='left', left_on=['Cvendedor'], right_on=['Cod_Empleado'])
nme3 = nme_dia.loc[nme_dia.Num_Documento.notna()]

# Grouping by 
gdfv_dia = df_v_dia.groupby(['Cod_Empleado', 'Desc_local']).agg({'Costo_Venta-Empleado':'sum'}).reset_index()

r4 = nme3.groupby(['Cod_Empleado', 'Num_Documento','nombre_completo', 'Desc_local','Cargo']).agg({'Cautoriza':'nunique',  'Qcantidad':'sum', 'Costo_NC-Empleado':'sum'}).reset_index()

r5 = r4.merge(gdfv_dia, how='left', on=['Cod_Empleado', 'Desc_local'])
r5['Costo_NC-Tienda'] = r5.groupby(['Desc_local'])['Costo_NC-Empleado'].transform('sum')
r5['Costo_Venta-Tienda'] = r5.groupby(['Desc_local'])['Costo_Venta-Empleado'].transform('sum')

r5['NC/Venta-Empleado'] = r5['Costo_NC-Empleado']/r5['Costo_Venta-Empleado']
r5['Costo_NC-Empleado/Costo_NC-Tienda'] = r5['Costo_NC-Empleado']/r5['Costo_NC-Tienda']
r5['CVenta-Empleado/CVenta-Tienda'] = r5['Costo_Venta-Empleado']/r5['Costo_Venta-Tienda']

# Notas crédito diarias mayores a 100 mil 
ncs_groupby = nme3.groupby(['Cautoriza', 'Desc_local', 'Dcompra_nvo', 'Nterminal_nvo','Nsecuencia_nvo', 'Hora', 'Cod_Empleado','Num_Documento', 'nombre_completo', 'Cargo']).agg({'SKU':'nunique', 'Qcantidad':'sum', 'Costo_NC-Empleado':'sum'}).reset_index()
ncs_groupby.loc[:, ['Grabación?', 'Cliente?', 'Producto?']] = [np.nan, np.nan, np.nan]
mayores_cienmil = ncs_groupby.loc[ncs_groupby['Costo_NC-Empleado']>=100000]

# Notas crédito diarias en horarios extraños 
r6 = ncs_groupby.loc[(ncs_groupby.Hora <=1000)|((ncs_groupby.Hora >=2100))]

def guardar_res_tienda(df_mes, df_dia, df_hora, df_monto, df_nc_daily, tienda, date):
       date_str = date.strftime('%y%m%d')
       path = rf'C:/Users/palejparra/Falabella/JPPs - Análisis de notas crédito - {tienda} - {tienda}'
       writer = pd.ExcelWriter(f'{path}/{date_str} {tienda}.xlsx', engine='xlsxwriter')
       df_hora.loc[df_hora.Desc_local == tienda].to_excel(writer, sheet_name='Alertas x hora', index=False)
       df_monto.loc[df_monto.Desc_local == tienda].to_excel(writer, sheet_name='Alertas x monto', index=False)
       df_dia.loc[df_dia.Desc_local == tienda].to_excel(writer, sheet_name='Empleados x Día', index=False)
       df_mes.loc[df_mes.Desc_local == tienda].to_excel(writer, sheet_name='Empleados x Mes', index=False)
       df_nc_daily.loc[df_nc_daily.Desc_local==tienda].to_excel(writer, sheet_name=f'NCs {date_str}', index=False)
       writer.save()

## -- Fin files teams

## -- Inicio webhook
dwt = data_nc.dict_webhook_tiendas
dst = data_nc.dict_sharepoint_tiendas
dct = data_nc.ciudades_tiendas

def send_msg(tienda, webhook, url_sp, ciudad, qty, sum, date):
    myTeamsMessage = pymsteams.connectorcard(webhook, verify=False)
    myTeamsMessage.title(f"Reporte de notas crédito | {date}")
    myTeamsMessage.text(f'Tienda: {tienda} - {ciudad}')

    section_1 = pymsteams.cardsection()
    section_1.addFact("Cantidad:", qty)
    section_1.addFact("Costo total:", sum)
    myTeamsMessage.addSection(section_1)

    myTeamsMessage.addLinkButton("Más información", url_sp)
    myTeamsMessage.send()

def send_msg_general(webhook, qty, sum, date):
    myTeamsMessage = pymsteams.connectorcard(webhook, verify=False)
    myTeamsMessage.title(f"Reporte de notas crédito | {date}")
    myTeamsMessage.text('Resumen tiendas')
    section_1 = pymsteams.cardsection()
    section_1.addFact("Cantidad:", qty)
    section_1.addFact("Costo total:", sum)
    myTeamsMessage.addSection(section_1)
    myTeamsMessage.send()


df_nc = pd.read_csv('input/ncs_aut/ncs/output/nc_df.csv', dtype='object')
df_nc.Mventa_nc = -pd.to_numeric(df_nc.Mventa_nc)

df_nc.Dcompra_nvo = df_nc.Dcompra_nvo.str.replace('oct', '10')
df_nc.Dcompra_nvo = df_nc.Dcompra_nvo.str.replace('nov', '11')
df_nc.Dcompra_nvo = df_nc.Dcompra_nvo.str.replace('dic', '12')

df_nc.Dcompra_nvo = pd.to_datetime(df_nc.Dcompra_nvo, format='%d-%m-%Y') # TODO arreglar para español 

nc = df_nc.loc[df_nc['Tipo_trx']=='NC'] # Transaction type 
local_excluir = ['3000', '2000', '11', '99', '321', '143']
nc = nc.loc[~nc.Local_creacion.isin(local_excluir)] # Local number 
#nc = nc.loc[nc['Cvendedor']!=47708] # Default seller 

nc.loc[nc['Desc_local']=='MARTINA COLINA', 'Desc_local'] = 'COLINA'
nc.loc[nc['Desc_local']=='MARTINA FONTANAR', 'Desc_local'] = 'FONTANAR'
nc.loc[nc['Desc_local']=='EXPO HAYUELOS', 'Desc_local'] = 'HAYUELOS'

df_ld = nc[nc.Dcompra_nvo == last_date]
ld_str = last_date.strftime('%d-%b-%y')
res = df_ld.groupby(['Desc_local']).agg({'Cautoriza':'nunique', 'Mventa_nc':'sum'})

for tienda, row in res.iterrows():
    monto = f'$ {row["Mventa_nc"]/1e6:,.1f} M'
    cantidad = str(row['Cautoriza'])
    send_msg(tienda, dwt[tienda], dst[tienda], dct[tienda], cantidad, monto, ld_str)
    guardar_res_tienda(r2, r5, r6, mayores_cienmil, nc_dia, tienda, last_date)

qty = str(res.Cautoriza.sum())
monto = f'$ {round(res.Mventa_nc.sum()/1e6)} M'

send_msg_general(dwt['GENERAL'], qty, monto, ld_str)

## -- Fin webhook