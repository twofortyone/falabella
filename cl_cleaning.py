import pandas as pd
from datetime import datetime
from unidecode import unidecode
import numpy as np

class CleaningText:

    def __init__(self) -> None:
        pass

    def normalizar_cols(bd):
        bd.rename(columns=lambda col: unidecode(col), inplace=True)
        bd.rename(columns=lambda col: col.strip('.!? \n\t').lower(), inplace=True)
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
        bd.loc[:,cols] = bd.loc[:,cols].apply(lambda x: x.str.strip('.!?$ \n\t').str.replace(
            '.', '', regex=False).str.replace(',', '.', regex=False) if x is not None else x)
        bd.loc[:,cols] = bd.loc[:,cols].apply(pd.to_numeric, errors='coerce', downcast='float')
        bd.loc[:,cols].fillna(0)
        return bd
        
    def limpiar_cols(bd, cols):
        bd.loc[:,cols].fillna('N/A', inplace=True)
        bd.loc[:,cols] = bd.loc[:,cols].apply(lambda x: x.str.strip('.!?$ \n\t').str.lower() if x is not None else x)
        bd.loc[:,cols].fillna('N/A', inplace=True)
        return bd