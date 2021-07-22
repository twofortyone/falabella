import pandas as pd 
import numpy as np 
import math

cf11_cd = pd.read_csv('210716-1327-novedades-cf11s_cd_20.csv', sep=',', dtype='object')
tab = pd.read_csv('tabla-cierres.csv', sep=',', dtype='object')

cf11_cd.total_costo_promedio = pd.to_numeric(cf11_cd.total_costo_promedio)

sum_gco = cf11_cd.groupby(['status_nuevo','GCO'])['total_costo_promedio'].sum().reset_index()
sum_nov = sum_gco[sum_gco.GCO!='OKK'].groupby('status_nuevo')['total_costo_promedio'].sum().reset_index()
sum_nov['GCO']='NOV'
res_gco = pd.concat([sum_gco[sum_gco.GCO=='OKK'], sum_nov])

totales = cf11_cd.groupby(['status_nuevo'])['total_costo_promedio'].sum().reset_index()

# Funciones 
def get_tcp(bd, args):
    return math.ceil(bd.loc[totales['status_nuevo'].isin(args),'total_costo_promedio'].sum()/1e6)

def escribir(pos1, pos2):
    return  '100% revisado contra SRX. ' + pos1.map(str) + ' de novedad equivalente a ' + pos2.map(str) + ' millones.'

# Casuisticas CD 
f4_db = ['error en cierre de f11', 'error en creacion de f11','f4 de merma','politica cambio agil','cierre x f4 dado baja crate prestamos']
f3_dp = ['cierre x f3 devuelto a proveedor']
recibido =['cierre x producto guardado despues de inventario']
f4_ct = ['cierre x f4 cobrado a terceros']
f4_rev = ['f4 en revision']
rec_20 = ['cierre x producto guardado antes de inventario']
f5 = ['producto en tienda']
recupero = [ 'cierre x recupero con cliente - refacturacion - base fal.com']
anudup = ['registro duplicado en base de datos', 'cierre x duplicidad (f11 con mismo f12+sku+cantidad)' ]
pend = ['f3 en revision', 'en revision ci', 'en revision de transporte','en revision de ubicacion logistica inversa','ingreso con sku trocado']
casus = [f4_db, f3_dp, recibido, f4_ct, f4_rev, rec_20, f5, recupero, anudup, anudup,[],[]]

# Totales casuisticas cd 
tcd = [] # Totales CD 
for casu in casus: 
    tcd.append(get_tcp(totales, casu))

tcd.append(math.ceil(totales['total_costo_promedio'].sum()/1e6))
tcd.append(9373)

# Novedades casuisticas Cd 
ncd = [] 
for casu in casus: 
    ncd.append(get_tcp(sum_nov, casu))
ncd.append(0)
ncd.append(0)

# Cargar data a tabla 
tab.iloc[:,1] = pd.Series(tcd)
lista_tiendas = list(tab.iloc[:,2].values)
tab.iloc[:,2] = pd.Series(lista_tiendas)
tab.iloc[:,3] = tab.iloc[:,1] + tab.iloc[:,2] # Total cd y tiendas
tab.iloc[:,4] = ((tab.iloc[:,3]/tab.iloc[13,3])*100).apply(lambda x: f'{math.ceil(x)}%') # Porcentaje 
tab.loc[:, 'CD_NOV'] = pd.Series(ncd)
tab.loc[:,'CD_NOV_P'] = ((tab.loc[:,'CD_NOV']/tab.iloc[:,1])*100).fillna(0).apply(lambda x: f'{x:.1f}%') # Porcentaje 
tab.iloc[10,1]= tab.iloc[13,1]-tab.iloc[12,1]
tab.iloc[10,2]= tab.iloc[13,2]-tab.iloc[12,2]
tab.iloc[:,[1,2,3,7,8]] = tab.iloc[:,[1,2,3,7,8]].applymap(lambda x: f'$ {x:,.0f}') # Formato de $ y ,
tab.loc[0:7,'VALIDACIÓN DE CONTROL A CD'] = escribir(tab.loc[0:7,'CD_NOV_P'], tab.loc[0:7,'CD_NOV'])

perdida_cd = [1,2,2,2,0,2,2,2,1,0,0,0,0,0]
 # 0 es cero, 1 es cd total , 2 es novedad 
    
vacia = []
for i,x in enumerate(perdida_cd): 
    if x==1:
        vacia.append(tab.iloc[i,1])
    elif x==2:
        vacia.append(tab.iloc[i,9])
    else: 
        vacia.append(0)
tab.iloc[:,7] = pd.Series(vacia)

tab.to_excel(f'tab1.xlsx', sheet_name='tab1',index=False) 

