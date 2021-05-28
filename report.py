class Report():

    def __init__(self, base) -> None:
        self.base = base

    def print_analysis(self, comp, comments, summary, total = None, nan=None, du=None, ne=None, nr=None, md=None, dc=None, anu=None, daa=None):
        print('\n--------------------------------------------------------------------')
        print(f'## Análisis con {comp} {comments}')
        print('--------------------------------------------------------------------')
        print('# Resumen:')
        if total!= None: print(f'= {total[0]} de {total[1]} registros con novedad, por un valor de: {total[2]:,.2f}')
        print('# Detalle: ')
        if nan!= None: print(f'- {nan[0]} de {nan[1]} registros cerrados sin número de {comp}, por un valor de {nan[2]:,.2f}')
        if du != None: print(f'- {du[0]} de {du[1]} registros duplicados, por un valor de {du[2]:,.2f}')
        if ne != None: print(f'- {ne[0]} de {ne[1]} registros no se encontraron en la base de {comp}, por un valor de {ne[2]:,.2f}')
        if nr != None: print(f'- {nr[0]} de {nr[1]} registros con estado de "no recibido" en la base de {comp}, por un valor de {nr[2]:,.2f}')
        if md != None: print(f'- {md[0]} de {md[1]} registros con estado de motivo de discrepancia en la base de {comp}, por un valor de {md[2]:,.2f}')
        if dc != None: print(f'- {dc[0]} de {dc[1]} registros no coinciden con cantidad de la base de {comp}, por un valor de {dc[2]:,.2f}')
        if anu != None: print(f'- {anu[0]} de {anu[1]} registros anulados en la base de {comp}, por un valor de {anu[2]:,.2f}')
        if daa != None: print(f'- {daa[0]} de {daa[1]} registros con año diferente a 2021 en la base de {comp}, por un valor de {daa[2]:,.2f}')
        if ~summary.empty: print(summary)
        print('--------------------------------------------------------------------')