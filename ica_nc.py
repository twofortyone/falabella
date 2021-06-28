import pandas as pd
from datetime import datetime
from ica_raw import InternalControlAnalysis

class CierresNC:

    def __init__(self, db, indexcol) -> None:
        """ 
        Get rows with different quantity 
        :param bd: dataframe base
        """
        self.db = db.copy()
        self.index_column = indexcol 
        self.dt_string = datetime.now().strftime('%y%m%d-%H%M%S')
        self.pcols = ['status-0', 'upc-1', 'cost-2', 'quantity-3']
        self.fcols = ['f3col-0', 'f4col-1', 'f5col-2', 'f11col-3', 'f12col-4', 'cod-nc-5']
        self.ica = InternalControlAnalysis(self.db, self.index_column)

    def set_fcols(self, fcols, pcols):
        self.pcols = pcols
        self.fcols = fcols

    def get_db(self):
        self.db = self.ica.get_db()
        return self.db

    def f3_verify(self, f3, status, yyyy, dqpcol):
        df1 = self.db[(self.db[self.pcols[0]]==status) & (self.db['esmc']=='cerrado')]
        df2= self.ica.get_fnan( df1, self.fcols[0], 'F3')
        if df2.empty == False: 
            df3 = self.ica.get_duplicates( df2,['cod_aut_nc', 'local_trx','upc', 'cantidad_trx_actual'], 'Cod Aut + Local + UPC + Qty')
            ne = self.ica.get_notfound( df3, f3, [self.fcols[0],self.pcols[1]], ['nro_devolucion','upc'], 'nro_devolucion', 'F3|UPC|QTY')
            df4 = pd.merge(df3, f3, left_on=[self.fcols[0],self.pcols[1]], right_on=['nro_devolucion','upc'])
            if df4.empty ==False: 
                df5 = self.ica.get_equalvalue(df4, 'descripcion6', 'Anulado', 'ANU', 'Registro anulado')
                comment = f'La cantidad sumada de los {dqpcol} de un f3 es mayor que la cantidad del f3'
                if df5.empty == False:
                    df6 = self.ica.get_diffqty_pro(df5, self.pcols[3], 'cantidad',dqpcol, 'nro_devolucion' ,comment)
                    iokf3 = df6[self.index_column].values
                    self.ica.update_db(iokf3,'GCO', 'OKK')
                    self.ica.update_db(iokf3,'Comentario GCO', 'Coincidencia exacta F3+UPC+QTY')
                    df7 = df6[df6['descripcion6']=='Confirmado']
                    df8= self.ica.get_diffvalue(df7, 'aaaa anulacion', yyyy, 'NAA', f'Registro con año de confirmación diferente a {yyyy}')

    def f4_verify(self, f4, status, yyyy):
        df1 = self.db[(self.db[self.pcols[0]]==status) & (self.db['esmc']=='cerrado')]
        df2 = self.ica.get_fnan( df1, self.fcols[1], 'F4')
        if df2.empty == False:
            df3 = self.ica.get_duplicates( df2, ['cod_aut_nc', 'local_trx','upc', 'cantidad_trx_actual'], 'Cod Aut + Local + UPC + Qty')
            ne = self.ica.get_notfound( df3, f4, [self.fcols[1], self.pcols[1]], ['nro_red_inventario','upc'], 'nro_red_inventario', 'F4|UPC|QTY')
            df4 = pd.merge(df3, f4, left_on=[self.fcols[1], self.pcols[1]], right_on=['nro_red_inventario','upc'])
            if df4.empty ==False: 
                df5 = self.ica.get_equalvalue(df4, 'estado', 'Anulado', 'ANU', 'Registro anulado')
                df6 = self.ica.get_diffvalue(df5, 'aa creacion', yyyy, 'NAA', f'Registro con año de creación diferente a {yyyy}')
                comment = 'Cantidad de las NCs de un F4 > cantidad del F4'
                if df6.empty == False:
                    df7 = self.ica.get_diffqty_pro(df6, self.pcols[3], 'cantidad',self.fcols[5],'nro_red_inventario', comment)
                    iokf4 = df7[self.index_column].values
                    self.ica.update_db(iokf4,'GCO', 'OKK')
                    self.ica.update_db(iokf4,'Comentario GCO', 'Coincidencia exacta F4+UPC+QTY')

    def f5_verify(self, f5, status, yyyy, dqpcol):
        df1 = self.db[(self.db[self.pcols[0]]==status) & (self.db['esmc']=='cerrado')]
        df2 = self.ica.get_fnan( df1, self.fcols[2], 'F5')
        if df2.empty ==False: 
            df3 = self.ica.get_duplicates( df2, ['cod_aut_nc', 'local_trx','upc', 'cantidad_trx_actual'], 'Cod Aut + Local + UPC + Qty')
            ne = self.ica.get_notfound( df3, f5, [self.fcols[2], self.pcols[1]], ['transfer','upc'], 'transfer', 'F5|UPC|Qty')
            df4 = pd.merge(df3, f5, left_on=[self.fcols[2], self.pcols[1]], right_on=['transfer','upc'])
            if df4.empty ==False: 
                df5 = self.ica.get_diffvalue(df4, 'estado', 'Recibido', 'NRE', 'Registro con estado diferente a recibido')
                df6 = self.ica.get_equalvalue(df5, 'motivo_discrepancia', 'F5 NO RECIBIDO', 'MDI', 'Registro con motivo de disc: F5 no recibido')
                df7 = self.ica.get_diffvalue(df6, 'aaaa reserva', yyyy, 'NAA', f'Registro con año de reserva diferente a {yyyy}')
                comment = 'Cantidad de las NCs de un F5 != (cantidad pickeada | cantidad recibida del F5)'
                if df7.empty == False: 
                    df8 = self.ica.get_diffqty_pro_f5(df7,  self.pcols[3], 'cant_recibida', dqpcol, 'transfer', comment)
                    iokf5 = df8[self.index_column].values
                    self.ica.update_db(iokf5, 'GCO','OKK')
                    self.ica.update_db(iokf5, 'Comentario GCO', 'Coincidencia exacta F5+UPC+QTY')

    def f5_verify_local(self, f5, status, yyyy, dqpcol, local):
        df1 = self.db[(self.db[self.pcols[0]]==status) & (self.db['esmc']=='cerrado')]
        df2 = self.ica.get_fnan( df1, self.fcols[2], 'F5')
        if df2.empty ==False: 
            df3 = self.ica.get_duplicates( df2, ['cod_aut_nc', 'local_trx','upc', 'cantidad_trx_actual'], 'Cod Aut + Local + UPC + Qty')
            ne = self.ica.get_notfound( df3, f5, [self.fcols[2], self.pcols[1]], ['transfer','upc'], 'transfer', 'F5|UPC|Qty')
            df4 = pd.merge(df3, f5, left_on=[self.fcols[2], self.pcols[1]], right_on=['transfer','upc'])
            if df4.empty ==False:
                df5 = self.ica.get_diffvalue(df4, 'estado', 'Recibido', 'NRE', 'Registro con estado diferente a recibido')
                df6 = self.ica.get_equalvalue(df5, 'motivo_discrepancia', 'F5 NO RECIBIDO', 'MDI', 'Registro con motivo de disc: F5 no recibido')
                df7 = self.ica.get_diffvalue(df6, 'aaaa reserva', yyyy, 'NAA', f'Registro con año de reserva diferente a {yyyy}')
                comment = 'Cantidad de las NCs de un F5 != (cantidad pickeada | cantidad recibida del F5)'
                if df7.empty == False: 
                    df8 = self.ica.get_diffqty_pro_f5(df7,  self.pcols[3], 'cant_recibida', dqpcol, 'transfer', comment)
                    df9 = self.ica.get_diffvalue(df8, 'local_recep', local, 'NCL', f'Registro con local diferente a {local}')
                    iokf5 = df9[self.index_column].values
                    self.ica.update_db(iokf5, 'GCO','OKK')
                    self.ica.update_db(iokf5, 'Comentario GCO', 'Coincidencia exacta F5+UPC+QTY')
    
    def f5_verify_local_list(self, f5, status, yyyy, dqpcol, local , locales):
        nil = []
        df1 = self.db[(self.db[self.pcols[0]]==status) & (self.db['esmc']=='cerrado')]
        df2 = self.ica.get_fnan( df1, self.fcols[2], 'F5')
        if df2.empty ==False: 
            df3 = self.ica.get_duplicates( df2, ['cod_aut_nc', 'local_trx','upc', 'cantidad_trx_actual'], 'Cod Aut + Local + UPC + Qty')
            ne = self.ica.get_notfound( df3, f5, [self.fcols[2], self.pcols[1]], ['transfer','upc'], 'transfer', 'F5|UPC|Qty')
            df4 = pd.merge(df3, f5, left_on=[self.fcols[2], self.pcols[1]], right_on=['transfer','upc'])
            if df4.empty ==False: 
                df5 = self.ica.get_diffvalue(df4, 'estado', 'Recibido', 'NRE', 'Registro con estado diferente a recibido')
                df6 = self.ica.get_equalvalue(df5, 'motivo_discrepancia', 'F5 NO RECIBIDO', 'MDI', 'Registro con motivo de disc: F5 no recibido')
                df7 = self.ica.get_diffvalue(df6, 'aaaa reserva', yyyy, 'NAA', f'Registro con año de reserva diferente a {yyyy}')
                comment = 'Cantidad de las NCs de un F5 != (cantidad pickeada | cantidad recibida del F5)'
                df8, lista = self.ica.get_notinlist(df7, 'local_recep', locales, 'NCL', f'Registro con local diferente a {local}')
                if df8.empty == False: 
                    df9 = self.ica.get_diffqty_pro_f5(df7,  self.pcols[3], 'cant_recibida', dqpcol, 'transfer', comment)
                    iokf5 = df9[self.index_column].values
                    self.ica.update_db(iokf5, 'GCO','OKK')
                    self.ica.update_db(iokf5, 'Comentario GCO', 'Coincidencia exacta F5+UPC+QTY')
                return lista 
            else:
                return nil 
        else: 
            return nil 