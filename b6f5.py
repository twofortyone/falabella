# Librerías
import pandas as pd
from datetime import datetime
from unidecode import unidecode
import numpy as np

# Configurar pandas
# Configura pandas para mostrar solo dos decimales
pd.set_option('float_format', '{:,.2f}'.format)
pd.set_option('max_columns', 70)

# Obtener fecha y hora
now = datetime.now()
dt_string = now.strftime('%y%m%d-%H%M%S')

# Importar F5 enviado, reservado y recibido
f5e = pd.read_csv('datasets/f5_enviado.csv', sep=';', dtype='object')
f5rec = pd.read_csv('datasets/f5_reservado.csv', sep=';', dtype='object')
f5res = pd.read_csv('datasets/f5_recibido.csv', sep=';', dtype='object')
lf5 = [f5e, f5rec, f5res]
f5 = pd.concat(lf5, axis=0)

b6 = pd.read_csv('datasets/base6.csv', sep=';', dtype='object')


def normalizar(bd):
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
    bd[cols] = bd[cols].apply(lambda x: x.str.strip('.!?$ \n\t').str.replace('.', '', regex=False).str.replace(',', '.', regex=False))
    bd[cols] = bd[cols].apply(pd.to_numeric, downcast='float')
    return bd


f5 = normalizar(f5)
b6 = normalizar(b6)
b6 = col_duplicados(b6, 'estado')
b6 = convertir_a_numero(b6, ['costo', 'costo total'])
b6.reset_index(inplace=True)
b6.rename(columns={'index': 'indice_b6'}, inplace=True)
