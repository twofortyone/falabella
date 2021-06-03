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
        bd[cols] = bd[cols].apply(lambda x: x.fillna('N/A').str.strip('.!?$ \n\t').str.replace(
            '.', '', regex=False).str.replace(',', '.', regex=False))
        bd[cols] = bd[cols].apply(pd.to_numeric, downcast='float')
        return bd
        
    def limpiar_cols(bd, cols):
        bd[cols] = bd[cols].apply(lambda x: x.str.strip('.!?$ \n\t').str.lower())
        return bd