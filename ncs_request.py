from os import rename
import pandas as pd 
import numpy as np 
import io

ep = pd.read_excel('input/ncs_aut/211020_empleados_planta.xlsx', dtype='object')
et = pd.read_excel('input/ncs_aut/211020_empleados_temporales.xlsx', dtype='object')

file = open('input/ncs_aut/211020_ncs.txt', 'r', encoding='ISO-8859-1')
nc_lines = file.readlines()
df_nc = pd.read_csv(io.StringIO("\n".join(nc_lines)), sep=';', dtype='object', error_bad_lines=False)

df_nc.Qcantidad = pd.to_numeric(df_nc.Qcantidad)
df_nc.Cvendedor = pd.to_numeric(df_nc.Cvendedor)
df_nc.Mventa_nc = -pd.to_numeric(df_nc.Mventa_nc)
ep.Cod_Empleado  = pd.to_numeric(ep.Cod_Empleado)

df_nc = df_nc.loc[:,['Local_creacion', 'Desc_local', 'Dcompra_nvo', 'Nterminal_nvo',
       'Nsecuencia_nvo', 'Hora', 'Tipo_trx', 'Cautoriza', 'Estado', 'Usuario',
       'Cvendedor', 'Local_ant', 'Descr_local_ant', 'Dcompra_ant',
       'Nterminal_ant', 'Nsecuencia_ant', 'Cvendedor_ant', 'Cajero_apertura',
       'Linea', 'LiDescripcion','SKU', 'EAN', 'PDescripcion',
       'Cmarca', 'Tipo Producto','Nrutcomprador', 'Qcantidad', 'Mventa_nc','Xtipificacion']]

df_nc.rename(columns={'Mventa_nc':'Costo_NC-Empleado'}, inplace=True)

nc = df_nc.loc[df_nc['Tipo_trx']=='NC'] 

local_excluir = ['3000', '2000', '11', '99']
nc = nc.loc[~nc.Local_creacion.isin(local_excluir)]

nc = nc.loc[nc['Cvendedor']!=47708]

ep['nombre_completo'] = ep['Nombre'] + ' ' + ep['Apell_Paterno']

ep = ep.loc[:,[ 'Cod_Sucursal', 'Sucursal', 'Cod_Empleado', 'Num_Documento', 'FecInicioContrato', 'Cod_Cargo', 'Cargo', 'Cod_Depto',
       'Departamento','Nombre_Superior', 'nombre_completo']]

et = et.loc[:,['SUCURSAL', 'APELLIDOS Y NOMBRES', 'DOCUMENTO', 'FECHA INGRESO', 'CARGO', 'FECHA TERMINO', 'TDA_AREA']]
et.rename(columns={'SUCURSAL':'Sucursal', 'APELLIDOS Y NOMBRES':'nombre_completo', 'DOCUMENTO':'Num_Documento', 'FECHA INGRESO':'FecInicioContrato', 'CARGO':'Cargo', 'TDA_AREA':'Departamento'}, inplace=True)

nme = nc.merge(ep, how='left', left_on=['Cvendedor'], right_on=['Cod_Empleado'])
nme2 = nme.loc[nme.Num_Documento.notna()]
#nme2.groupby(['Cod_Empleado', 'Num_Documento','nombre_completo', 'Desc_local', 'Cargo'])['Cautoriza'].nunique().nlargest(10)
r1 = nme2.groupby(['Cod_Empleado', 'Num_Documento','nombre_completo', 'Desc_local','Cargo']).agg({'Cautoriza':'nunique',  'Qcantidad':'sum', 'Costo_NC-Empleado':'sum'}).reset_index()


# Ventas 
df_v = pd.read_excel('input/ncs_aut/211020_ventas.xlsx', dtype='object')
df_v['Día'] = pd.to_datetime(df_v['Día'], format='%d/%m/%Y')
df_v['Número de Vendedor (Cod.)'] = pd.to_numeric(df_v['Número de Vendedor (Cod.)'])
df_v['Venta en $'] = pd.to_numeric(df_v['Venta en $'])
df_v = df_v.loc[df_v['Día'] == '2021-09-20']
df_v.rename(columns={'Número de Vendedor (Cod.)':'Cod_Empleado', 'Local':'Desc_local', 'Venta en $':'Costo_Venta-Empleado' }, inplace=True)
gdfv = df_v.groupby(['Cod_Empleado', 'Desc_local']).agg({'Costo_Venta-Empleado':'sum'}).reset_index()

r2 = r1.merge(gdfv, how='left', on=['Cod_Empleado', 'Desc_local'])
r2['Costo_NC-Tienda'] = r2.groupby(['Desc_local'])['Costo_NC-Empleado'].transform('sum')
r2['Costo_Venta-Tienda'] = r2.groupby(['Desc_local'])['Costo_Venta-Empleado'].transform('sum')

r2['NC/Venta-Empleado'] = r2['Costo_NC-Empleado']/r2['Costo_Venta-Empleado']
r2['Costo_NC-Empleado/Costo_NC-Tienda'] = r2['Costo_NC-Empleado']/r2['Costo_NC-Tienda']
r2['CVenta-Empleado/CVenta-Tienda'] = r2['Costo_Venta-Empleado']/r2['Costo_Venta-Tienda']

nme2.to_excel('output/nc_base.xlsx', index=False)
#r1.to_excel('output/nc_test.xlsx', index=False)
r2.to_excel('output/nc_res.xlsx', index=False)