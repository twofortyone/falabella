import pandas as pd
from datetime import datetime

# Variables 
dt_string = datetime.now().strftime('%y%m%d-%H%M%S')

enero = pd.read_csv(f'input/nc_3000/210709/data/enero.csv', sep=';', dtype='object')
febrero= pd.read_csv(f'input/nc_3000/210709/data/febrero.csv', sep=';', dtype='object')
marzo= pd.read_csv(f'input/nc_3000/210709/data/marzo.csv', sep=';', dtype='object')
abril= pd.read_csv(f'input/nc_3000/210709/data/abril.csv', sep=';', dtype='object')
mayo= pd.read_csv(f'input/nc_3000/210709/data/mayo.csv', sep=';', dtype='object')
junio= pd.read_csv(f'input/nc_3000/210709/data/junio.csv', sep=';', dtype='object')

meses = [enero, febrero, marzo, abril, mayo, junio]

nc = pd.concat(meses, axis=0)

nc.to_csv(f'output/{dt_string}-nc.csv', sep=';', index=False)
