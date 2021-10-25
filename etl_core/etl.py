import pandas as pd 

class ETL(): 

    def __init__(self, input_folder) -> None:
        self.input_folder = input_folder

    def load_data(self, names):
        data = []
        pre_file = input('Ingrese prefijo de archivos: ')
    
        for name in names:
            data.append(pd.read_csv(f'{self.input_folder}{pre_file}{name}.csv', sep=';', dtype='object'))

        return data 

def compare_dfs(df1, df2):
    #identify new rows
    #compare old rows
    #join modified old rows with new ones 
    #return new, old equeal, old modified lists 
    pass