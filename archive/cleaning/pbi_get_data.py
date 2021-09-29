import pandas as pd
from pandas.io.parsers import read_csv 
import input.pbi_data_cons as const 
from cl_cleaning import CleaningText as ct 

df = pd.read_excel('input/db-pbi/210910_Consolidado_NC.xlsx', sheet_name='Hoja1', dtype='object')
df = ct.norm_header(df)

def drop_except(df, cols):
    df.drop(df.columns.difference(cols), axis=1, inplace=True)
    return df 


#df = drop_except(df)
nc_list = const.nc_lists
df.loc[:, nc_list[0]] = df.loc[:, nc_list[0]].apply(ct.clean_str)

df.to_csv('output/dc_nc.csv', index=False)

dc = pd.read_csv('output/dc_nc.csv', dtype='object')

dc.loc[:, nc_list[1]] = dc.loc[:, nc_list[1]].apply(ct.clean_fnum)
dc.loc[:, nc_list[2]] = dc.loc[:, nc_list[2]].apply(pd.to_numeric)
dc.loc[:, nc_list[2]] = dc.loc[:, nc_list[2]].apply(lambda x: x.fillna(0))
dc.loc[:, nc_list[2]] = dc.loc[:, nc_list[2]].apply(lambda x: x.astype(int))

dc.qcantidad = -dc.qcantidad
dc.mventa_nc = -dc.mventa_nc
dc.to_csv('output/data_consolidado_nc.csv', sep=';', index=False)