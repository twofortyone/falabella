import pandas as pd
from datetime import datetime

# Variables 
dt_string = datetime.now().strftime('%y%m%d-%H%M')

filename= '210730_cnc_base'
print('Loading month 1')
enero = pd.read_excel(f'input/bases/{filename}.xlsx', sheet_name='enero', dtype='object')
print('Loading month 2')
febrero= pd.read_excel(f'input/bases/{filename}.xlsx', sheet_name='febrero', dtype='object')
print('Loading month 3')
marzo= pd.read_excel(f'input/bases/{filename}.xlsx', sheet_name='marzo', dtype='object')
print('Loading month 4')
abril= pd.read_excel(f'input/bases/{filename}.xlsx', sheet_name='abril', dtype='object')
print('Loading month 5')
mayo= pd.read_excel(f'input/bases/{filename}.xlsx', sheet_name='mayo', dtype='object')
print('Loading month 6')
junio= pd.read_excel(f'input/bases/{filename}.xlsx', sheet_name='junio', dtype='object')

meses = [enero, febrero, marzo, abril, mayo, junio]
print('Concate months')
nc = pd.concat(meses, axis=0)
print('Saving NCs')
path1 = f'output/bases/{dt_string}-cnc.csv'
path2 = f'output/bases/{dt_string}-cnc.xlsx'
nc.to_csv(path1, sep=';', index=False)
nc.to_excel(path2, sheet_name=f'{dt_string}-cnc',index=False) 
print(path1)
print(path2)