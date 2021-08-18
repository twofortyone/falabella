import pandas as pd
from datetime import datetime

# Variables 
dt_string = datetime.now().strftime('%y%m%d-%H%M')

filename= '210805_cnc_20'
print('Loading DB 1')
B1 = pd.read_excel(f'input/bases/{filename}.xlsx', sheet_name='B1', dtype='object')
print('Loading DB 2')
B2 = pd.read_excel(f'input/bases/{filename}.xlsx', sheet_name='B2', dtype='object')
print('Loading DB 3')
B3= pd.read_excel(f'input/bases/{filename}.xlsx', sheet_name='B3', dtype='object')
print('Loading DB 4')
B4= pd.read_excel(f'input/bases/{filename}.xlsx', sheet_name='B4', dtype='object')
print('Loading DB 5')
B5= pd.read_excel(f'input/bases/{filename}.xlsx', sheet_name='B5', dtype='object')
print('Loading DB 6')
B6= pd.read_excel(f'input/bases/{filename}.xlsx', sheet_name='B6', dtype='object')
print('Loading DB 7')
B7= pd.read_excel(f'input/bases/{filename}.xlsx', sheet_name='B7', dtype='object')

meses = [B1, B2, B3, B4, B5, B6, B7]
print('Concate DBs')
nc = pd.concat(meses, axis=0)
print('Saving NCs')
path1 = f'output/bases/{dt_string}-cnc_20.csv'
path2 = f'output/bases/{dt_string}-cnc_20.xlsx'
nc.to_csv(path1, sep=';', index=False)
nc.to_excel(path2, sheet_name=f'{dt_string}-cnc',index=False) 
print(path1)
print(path2)