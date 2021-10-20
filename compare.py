from os import sep
import pandas as pd 
import numpy as np 

prev_result = pd.read_excel('211005-0953-cf11_cd_21-output.xlsx', sheet_name='DB') # Previous result
df_base = pd.read_csv('211019-1504-cf11_cd_21.csv', sep=';') # New database cf11

lista_cols = ['nfolio', 'prd_upc', 'qproducto','xservicio','costo_total', 'f12', 'status_final', 'f3', 'f4', 'f5','f11_nuevo']

pr = prev_result[lista_cols] # Previous result with required cols 
df = df_base[lista_cols] # New database with required cols 

records = df.loc[df.nfolio.isin(pr.nfolio)] # Records from new database to compare with 
new_records = df.loc[~df.nfolio.isin(pr.nfolio)] # New records 

comparison = records.compare(pr) # Comparison btw same records from new database and previous result

modified = records.loc[comparison.index]
unmodified = records.loc[~records.index.isin(comparison.index)]

df_base.loc[new_records.index,'c_indicator'] = 'nr'
df_base.loc[modified.index,'c_indicator'] = 'mo'
df_base.loc[unmodified.index,'c_indicator'] = 'un'

pr_aux = prev_result.drop_duplicates(['nfolio', 'prd_upc', 'qproducto', 'status_final'])
df_merged = df_base.merge(pr_aux[['nfolio', 'prd_upc', 'qproducto', 'status_final', 'GCO', 'Comentario GCO']], how='left', on=['nfolio', 'prd_upc','qproducto','status_final'])
df_merged.loc[(df_merged.c_indicator =='nr')|(df_merged.c_indicator =='mo'), ['GCO', 'Comentario GCO']] = np.nan
#new_records.to_excel('new_records.xlsx')
#modified.to_excel('modified.xlsx')
#unmodified.to_excel('unmodified.xlsx')
comparison.to_excel('comparison.xlsx')
df_merged.to_excel('df_base.xlsx', index=False)
df_merged.to_csv('df_base.csv', sep=';', index=False)