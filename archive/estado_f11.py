from os import sep
import pandas as pd 
from unidecode import unidecode

base = pd.read_csv('output/bases/210803-0816-210803_Respuestas_SAC_210727-1145-cf11_tienda-output.csv', sep=';', dtype='object')
f11 = pd.read_csv('output/bases/210803-0751-210803_Ajustes_Toma_de_Inventario-FALL-PC0GYV9W.csv', sep=';', dtype='object')

fax= f11[['NFOLIO', 'ESTADO ACTUAL DEL F11 - 2 AGO']]

fax = fax.rename(columns={'ESTADO ACTUAL DEL F11 - 2 AGO':'estado_f11'})
save = base.merge(fax, how='left', on=['NFOLIO'])

save = save.rename(columns={'NFOLIO':'nfolio', 'PRD_UPC':'prd_upc'})
def clean_str(col):
    res = col.fillna('nan')
    res = res.apply(unidecode)
    res = res.str.replace(r'([^a-zA-Z0-9-+(). ])', '', regex=True)
    res = res.str.strip()
    res = res.str.lower()
    return res 

save['estado_f11'] = clean_str(save['estado_f11'])
save.to_csv(f'output/bases/210803-cf11_tienda_20-estado.csv', sep=';', decimal=',', index=False)