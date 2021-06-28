import pandas as pd
from ica_raw import InternalControlAnalysis

class CierresF11:

    def __init__(self, db, index_name) -> None:
        self.db = db 
        self.index_column = index_name
        self.pcols = ['status-0', 'upc-1', 'cost-2', 'quantity-3']
        self.fcols = ['f3col-0', 'f4col-1', 'f5col-2', 'f11col-3', 'f12col-4']
        self.ica = InternalControlAnalysis(self.db, self.index_column)

    def set_fcols(self, fcols, pcols ):
        self.fcols = fcols
        self.pcols = pcols

    def get_db(self):
        self.db = self.ica.get_db()
        return self.db 

    def f4_verify(self, f4, status, yyyy):
        df1 = self.db[self.db[self.pcols[0]]==status]
        df2 = self.ica.get_fnan( df1, self.fcols[1], 'F4')
        if df2.empty == False:
            df3 = self.ica.get_duplicates( df2, [self.fcols[4], self.pcols[1], self.pcols[3]], 'F12 + UPC + Cantidad')
            ne = self.ica.get_notfound( df3, f4, [self.fcols[1], self.pcols[1]], ['nro_red_inventario','upc'], 'nro_red_inventario', 'F4|UPC|QTY')
            df4 = pd.merge(df3, f4, left_on=[self.fcols[1], self.pcols[1]], right_on=['nro_red_inventario','upc'])
            if df4.empty ==False: 
                df5 = self.ica.get_equalvalue(df4, 'estado', 'Anulado', 'ANU', 'Registro anulado')
                df6 = self.ica.get_diffvalue(df5, 'aa creacion', yyyy, 'NAA', f'Registro con año de creación diferente a {yyyy}')
                df7 = self.ica.get_diffqty_pro(df6, self.pcols[3], 'cantidad',self.fcols[3],'nro_red_inventario', 'La cantidad sumada de los F11s de un F4 es mayor que la cantidad del F4')
                iokf4 = df7[self.index_column].values
                self.ica.update_db(iokf4,'GCO', 'OKK')
                self.ica.update_db(iokf4,'Comentario GCO', 'Coincidencia exacta F4+UPC+QTY')

    def f5_verify(self, f5, status, yyyy, ):
        df1 = self.db[self.db[self.pcols[0]]==status]
        df2 = self.ica.get_fnan( df1, self.fcols[2], 'F5')
        if df2.empty ==False: 
            df3 = self.ica.get_duplicates( df2, [self.fcols[4], self.pcols[1], self.pcols[3] ], 'F12 + UPC + Cantidad')
            ne = self.ica.get_notfound( df3, f5, [self.fcols[2], self.pcols[1]], ['transfer','upc'], 'transfer', 'F5|UPC|Qty')
            df4 = pd.merge(df3, f5, left_on=[self.fcols[2], self.pcols[1]], right_on=['transfer','upc'])
            if df4.empty ==False: 
                df5 = self.ica.get_diffvalue(df4, 'estado', 'Recibido', 'NRE', 'Registro con estado diferente a recibido')
                df6 = self.ica.get_equalvalue(df5, 'motivo discrepancia', 'F5 NO RECIBIDO', 'MDI', 'Registro con motivo de disc: F5 no recibido')
                df7 = self.ica.get_diffvalue(df6, 'aaaa reserva', yyyy, 'NAA', f'Registro con año de reserva diferente a {yyyy}')
                comment = f'La cantidad sumada de los F11s de un F5 es mayor que la cantidad del F5'
                df8 = self.ica.get_diffqty_pro(df7,  self.pcols[3], 'can_recibida', self.fcols[3], 'transfer', comment)
                iokf5 = df8[self.index_column].values
                self.ica.update_db(iokf5, 'GCO','OKK')
                self.ica.update_db(iokf5, 'Comentario GCO', 'Coincidencia exacta F5+UPC+QTY')

    def f3_verify(self, f3, status, yyyy):
        df1 = self.db[self.db[self.pcols[0]]==status]
        df2= self.ica.get_fnan( df1, self.fcols[0], 'F3')
        if df2.empty == False: 
            df3 = self.ica.get_duplicates( df2,[self.fcols[4],self.pcols[1], self.pcols[3]], 'F12 + UPC + Cantidad')
            ne = self.ica.get_notfound( df3, f3, [self.fcols[0],self.pcols[1]], ['nro_devolucion','upc'], 'nro_devolucion', 'F3|UPC|QTY')
            df4 = pd.merge(df3, f3, left_on=[self.fcols[0],self.pcols[1]], right_on=['nro_devolucion','upc'])
            if df4.empty ==False: 
                df5 = self.ica.get_equalvalue(df4, 'descripcion6', 'Anulado', 'ANU', 'Registro anulado')
                df6 = self.ica.get_diffqty_pro(df5, self.pcols[3], 'cantidad',self.fcols[3], 'nro_devolucion' ,'La cantidad sumada de los f11s de un f3 es mayor que la cantidad del f3')
                iokf3 = df6[self.index_column].values
                self.ica.update_db(iokf3,'GCO', 'OKK')
                self.ica.update_db(iokf3,'Comentario GCO', 'Coincidencia exacta F3+UPC+QTY')
                df7 = df6[df6['descripcion6']=='Confirmado']
                df8= self.ica.get_diffvalue(df7, 'aaaa anulacion', yyyy, 'NAA', f'Registro con año de confirmación diferente a {yyyy}')

    def kpi_verify(self, kpi, status, yyyy, commenty):
        df1 = self.db[self.db[self.pcols[0]]==status]
        df2= self.ica.get_fnan_cols(df1, [self.fcols[4],self.fcols[3]], 'KPID')
        df3 = self.ica.get_duplicates( df2, [self.fcols[4],'prd_upc', 'qproducto'], 'F12 + UPC + Cantidad')

        index_ne_kpi_di = self.ica.get_notfound( df3, kpi, [self.fcols[4]], ['entrada'], 'entrada', '(F12|F11)+UPC+QTY')
        index_ne_kpi_di2 = self.ica.get_notfound( self.db.loc[index_ne_kpi_di], kpi, [self.fcols[3]], ['entrada'], 'entrada', '(F12|F11)+UPC+QTY')

        pgdim1 = pd.merge(df3, kpi, left_on=[self.fcols[4]], right_on=['entrada'])
        pgdim2 = pd.merge(df3.loc[index_ne_kpi_di], kpi, left_on=[self.fcols[3]], right_on=['entrada'])
        lpgdi = [pgdim1, pgdim2]
        pgdim = pd.concat(lpgdi, axis=0)
        pgdimdyear = '' 
        if yyyy == '2021': 
            pgdimdyear = self.ica.get_diffvalue(pgdim, 'aaaa_paletiza', yyyy, 'NAA',commenty)
        else:
            pgdimdyear = self.ica.get_menorvalue(pgdim, 'fecha_paletiza', '2021-01-20', 'NAA', commenty)
        iokkpid = pgdimdyear[self.index_column].values
        self.ica.update_db(iokkpid,'GCO', 'OKK')
        self.ica.update_db(iokkpid,'Comentario GCO', 'Coincidencia exacta (F12|F11)+UPC+QTY')

    def refact_verify(self, refact, status):
        df1 = self.db[self.db[self.pcols[0]]==status]
        df2= self.ica.get_fnan( df1, self.fcols[4], 'F12-REFACT')
        df3 = self.ica.get_duplicates( df2,[self.fcols[4],'prd_upc', 'qproducto'], 'F12 + UPC + Cantidad')
        ne = self.ica.get_notfound( df3, refact, [self.fcols[4]], ['f12cod'], 'f12cod', 'F12')
        df4 = pd.merge(df3, refact, left_on=[self.fcols[4]], right_on=['f12cod'])
        df5 = self.ica.get_equalvalue(df4, 'confirmacion_tesoreria', 'NO REINTEGRADO - TRX DECLINADA', 'ANU', 'Registro con TRX declinada')
        #df5 = cierres.ica.get_diffvalue(df4, 'estado', 'APPROVED', 'ANU', 'Registro con transacción anulada')
        # df6 = cierres.ica.get_diffqty_pro(df5, 'qproducto', 'cantidad',f11_col, f3_col,'La cantidad sumada de los f11s de un f3 es mayor que la cantidad del f3')
        iokf12 = df5[self.index_column].values
        self.ica.update_db(iokf12,'GCO', 'OKK')
        self.ica.update_db(iokf12,'Comentario GCO', 'Coincidencia exacta')

    def finals(self):
        self.db = self.ica.get_db()
        self.db.loc[self.db['GCO'].notna(), 'CH'] = 'DC'
        self.db.loc[self.db['GCO'].isna(), 'CH'] = 'BL'