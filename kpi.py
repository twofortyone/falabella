# Librerías
from os import sep
import pandas as pd
from datetime import datetime

# Variables
now = datetime.now()
dt_string = now.strftime("%y%m%d-%H%M%S")

kpi = pd.read_csv('input/kpi.csv', sep=';', dtype='object')

kpi.entrada.str.extract('(\d+)')