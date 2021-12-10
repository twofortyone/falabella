import config.nc_webhook as data_nc
import pymsteams
from datetime import datetime
import pandas as pd 

dwt = data_nc.dict_webhook_tiendas
dst = data_nc.dict_sharepoint_tiendas
dct = data_nc.ciudades_tiendas

def send_msg(tienda, webhook, url_sp, ciudad, qty, sum, date):
    myTeamsMessage = pymsteams.connectorcard(webhook, verify=False)
    myTeamsMessage.title(f"Reporte de notas crédito | {date}")
    myTeamsMessage.text(f'Tienda: {tienda} - {ciudad}')

    section_1 = pymsteams.cardsection()
    section_1.addFact("Cantidad:", qty)
    section_1.addFact("Costo total:", sum)
    myTeamsMessage.addSection(section_1)

    myTeamsMessage.addLinkButton("Más información", url_sp)
    myTeamsMessage.send()

def send_msg_general(webhook, qty, sum, date):
    myTeamsMessage = pymsteams.connectorcard(webhook, verify=False)
    myTeamsMessage.title(f"Reporte de notas crédito | {date}")
    myTeamsMessage.text('Resumen tiendas')
    section_1 = pymsteams.cardsection()
    section_1.addFact("Cantidad:", qty)
    section_1.addFact("Costo total:", sum)
    myTeamsMessage.addSection(section_1)
    myTeamsMessage.send()


df_nc = pd.read_csv('input/ncs_aut/ncs/output/nc_df.csv', dtype='object')
df_nc.Mventa_nc = -pd.to_numeric(df_nc.Mventa_nc)

df_nc.Dcompra_nvo = df_nc.Dcompra_nvo.str.replace('oct', '10')
df_nc.Dcompra_nvo = df_nc.Dcompra_nvo.str.replace('nov', '11')
df_nc.Dcompra_nvo = df_nc.Dcompra_nvo.str.replace('dic', '12')

df_nc.Dcompra_nvo = pd.to_datetime(df_nc.Dcompra_nvo, format='%d-%m-%Y') # TODO arreglar para español 

nc = df_nc.loc[df_nc['Tipo_trx']=='NC'] # Transaction type 
local_excluir = ['3000', '2000', '11', '99', '321', '143']
nc = nc.loc[~nc.Local_creacion.isin(local_excluir)] # Local number 
#nc = nc.loc[nc['Cvendedor']!=47708] # Default seller 

nc.loc[nc['Desc_local']=='MARTINA COLINA', 'Desc_local'] = 'COLINA'
nc.loc[nc['Desc_local']=='MARTINA FONTANAR', 'Desc_local'] = 'FONTANAR'
nc.loc[nc['Desc_local']=='EXPO HAYUELOS', 'Desc_local'] = 'HAYUELOS'

last_date = nc.Dcompra_nvo.max().strftime('%d-%b-%y')
df_ld = nc[nc.Dcompra_nvo == last_date]
res = df_ld.groupby(['Desc_local']).agg({'Cautoriza':'nunique', 'Mventa_nc':'sum'})

for tienda, row in res.iterrows():
    monto = f'$ {row["Mventa_nc"]/1e6:,.1f} M'
    cantidad = str(row['Cautoriza'])
    send_msg(tienda, dwt[tienda], dst[tienda], dct[tienda], cantidad, monto, last_date)

qty = str(res.Cautoriza.sum())
monto = f'$ {round(res.Mventa_nc.sum()/1e6)} M'

send_msg_general(dwt['GENERAL'], qty, monto, last_date)