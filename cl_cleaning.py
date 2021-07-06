import pandas as pd
from datetime import datetime
from unidecode import unidecode
import numpy as np

class CleaningText:

    def __init__(self) -> None:
        pass

    def norm_header(bd):
        bd.rename(columns=lambda col: unidecode(col), inplace=True)
        bd.rename(columns=lambda col: col.strip('.!? \n\t').lower(), inplace=True)
        bd.rename(columns=lambda col: col.replace('.', '').replace(' ', '_'), inplace = True)
        return bd


    def col_duplicados(bd, col):
        cols = []
        n = 1
        for col in bd.columns:
            if col == 'estado':
                cols.append(f'estado_{n}')
                n += 1
                continue
            cols.append(col)
        bd.columns = cols
        return bd

    def convertir_a_numero(bd, cols):
        bd.loc[:,cols].fillna('N/A', inplace=True)
        bd.loc[:,cols] = bd.loc[:,cols].apply(lambda x: x.str.strip('-.!?$ \n\t').str.replace(
            '.', '', regex=False).str.replace(',', '.', regex=False).str.strip() )
        #print(bd.loc[0,cols])
        bd.loc[:,cols] = bd.loc[:,cols].apply(pd.to_numeric)
        bd.loc[:,cols].fillna(0)
        return bd
        
    def limpiar_cols(bd, cols):
        #bd.loc[:,cols].fillna('N/A', inplace=True)
        bd.loc[:,cols] = bd.loc[:,cols].apply(lambda x: x.str.strip('.!?$ \n\t').str.lower())
        #bd.loc[:,cols].fillna('N/A', inplace=True)
        return bd

    def strip_symbols(col):
        return col.str.strip('.!?$ \n\t')
    
    def lower_col(col):
        return col.str.lower()

    def to_number(col ):
        res = col.str.strip('.!?$ \n\t')
        res = res.str.replace('.','', regex=False)
        # Si f no es dígito => nan 
        res = res.fillna('N/A')
        res.loc[~res.str.isdigit()] = np.nan
        # Convertir a num 
        res = pd.to_numeric(res)
        return res 

    def clean_fnum(col):
        res = col.str.strip('.!?$ \n\t')
        res = res.str.replace('.','', regex=False)
        # Si f = 0 => nan 
        res.loc[res=='0'] = np.nan
        # Si f no es dígito => nan 
        res = res.fillna('N/A')
        res.loc[~res.str.isdigit()] = np.nan
        return res 
    
    def clean_num(col):
        res = col.str.strip('.!?$ \n\t')
        res = res.str.replace('.','', regex=False)
        # Si f no es dígito => nan 
        res = res.fillna('N/A')
        res.loc[~res.str.isdigit()] = np.nan
        return res 