import collections
import pandas as pd 
from cl_cleaning import CleaningText as ct 
b1 = pd.read_csv(f'input/ajustes_3000/210528/base1.csv', sep=';', dtype='object')
b2 = pd.read_csv(f'input/ajustes_3000/210528/base2345.csv', sep=';', dtype='object')
b6 = pd.read_csv(f'input/ajustes_3000/210528/base6.csv', sep=';', dtype='object')
b7 = pd.read_csv(f'input/ajustes_3000/210528/base7.csv', sep=';', dtype='object')

b1 = b1.rename(columns={'COSTO POR CANTIDAD':'costo_total'})
b1 = ct.normalizar_cols(b1)

b2 = b2.rename(columns={'TOTAL':'costo_total'})
b2 = ct.normalizar_cols(b2)

b6 = b6.rename(columns={'COSTO TOTAL':'costo_total'})
b6 = ct.normalizar_cols(b6)


b7 = ct.normalizar_cols(b7)

b1aux = b1.loc[:,['sku', 'unidades','costo_total']]
b2aux = b2.loc[:,['sku','unidades', 'costo_total']]
b6aux = b6.loc[:,['sku','unidades', 'costo_total']]

lista = [b1aux, b2aux, b6aux]

df = pd.concat(lista, axis=0, ignore_index=True)

df = ct.convertir_a_numero(df,['costo_total'])