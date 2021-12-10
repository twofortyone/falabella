from os import sep
import pandas as pd 
import numpy as np 
from datetime import datetime


dt_string = datetime.now().strftime('%y%m%d-%H%M')

prev_result = pd.read_excel('rev_comparison/211129-1455_cf11_cd_21-report.xlsx', sheet_name='211129_cf11_cd_21') # Previous result
df_base = pd.read_excel('rev_comparison/211129-1637-cf11_cd_21-output.xlsx')
#df_base = pd.read_csv('rev_comparison/211108-1226-cf11_cd_21.csv', sep=';') # New database cf11

lista_cols = ['nfolio', 'prd_upc', 'qproducto','xservicio','costo_total', 'f12', 'status_final', 'f3', 'f4', 'f5','f11_nuevo']

pr = prev_result[lista_cols] # Previous result with required cols 
df = df_base[lista_cols] # New database with required cols 

records = df.loc[df.nfolio.isin(pr.nfolio)] # Records from new database to compare with 
new_records = df.loc[~df.nfolio.isin(pr.nfolio)] # New records 

comparison = records.compare(pr) # Comparison btw same records from new database and previous result

comparison_wo_price = comparison.loc[:, 'f12':'f11_nuevo']
comparison_wo_price.isna().all(axis=1)

only_price_changed = comparison[comparison_wo_price.isna().all(axis=1)]
other_cols_changed = comparison[~comparison_wo_price.isna().all(axis=1)]

modified = records.loc[comparison.index]
unmodified = records.loc[~records.index.isin(comparison.index)]

df_base.loc[new_records.index,'c_indicator'] = 'nr'
df_base.loc[other_cols_changed.index,'c_indicator'] = 'mo'
df_base.loc[only_price_changed.index,'c_indicator'] = 'un'
df_base.loc[unmodified.index,'c_indicator'] = 'un'

pr_aux = prev_result.drop_duplicates(['nfolio', 'prd_upc', 'qproducto', 'status_final'])
df_merged = df_base.merge(pr_aux[['nfolio', 'prd_upc', 'qproducto', 'status_final', 'GCO', 'Comentario GCO']], how='left', on=['nfolio', 'prd_upc','qproducto','status_final'])
df_merged.loc[(df_merged.c_indicator =='nr')|(df_merged.c_indicator =='mo'), ['GCO', 'Comentario GCO']] = np.nan

df_to_look = pd.concat([df_base.loc[new_records.index],df_base.loc[other_cols_changed.index]])
df_to_look.to_csv('rev_comparison/211025_df_to_look.csv', sep=';', index=False)
#new_records.to_excel('new_records.xlsx')
#modified.to_excel('modified.xlsx')
#unmodified.to_excel('unmodified.xlsx')
comparison.to_excel(f'rev_comparison/{dt_string}_comparison.xlsx')
df_merged.to_excel(f'rev_comparison/{dt_string}_df_output.xlsx', index=False)
df_merged.to_csv(f'rev_comparison/{dt_string}_df_output.csv', sep=';', index=False)

# merged 
res = df_merged.merge(comparison, how='left', left_index=True, right_index=True)
res.to_excel(f'rev_comparison/{dt_string}_df_w_comparsion-o.xlsx', index=False)