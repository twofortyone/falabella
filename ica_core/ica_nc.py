import pandas as pd
from datetime import datetime
from ica_core.ica_raw import InternalControlAnalysis

class CierresNC:

    def __init__(self, db) -> None:
        """ 
        Get rows with different quantity 
        :param bd: dataframe base
        """
        self.db = db.copy()
        self.dt_string = datetime.now().strftime('%y%m%d-%H%M%S')
        self.pcols = ['indice_nc-0','tipificacion-1', 'upc-2', 'cost-3', 'quantity-4', 'estado-5']
        self.fcols = ['f3col-0', 'f4col-1', 'f5col-2', 'f11col-3', 'f12col-4', 'cod-nc-5']
        self.ica = InternalControlAnalysis(self.db, 'indice_cnc')

    def set_fcols(self, fcols, pcols):
        self.pcols = pcols
        self.fcols = fcols

    def f3_verify(self, f3, status, yyyy):
        df1 = self.db[(self.db[self.pcols[1]]==status) & (self.db[self.pcols[5]]=='cerrado')]
        df2= self.ica.get_fnan( df1, self.fcols[0], 'F3')
        if df2.empty == False: 
            df3 = self.ica.get_duplicates( df2,[self.fcols[5], 'local_trx',self.pcols[2], self.pcols[4]], 'Cod Aut + Local + UPC + Qty')
            ne = self.ica.get_notfound( df3, f3, [self.fcols[0],self.pcols[2]], ['nro_devolucion',self.pcols[2]], 'nro_devolucion', 'F3|UPC|QTY')
            df4 = pd.merge(df3, f3, left_on=[self.fcols[0],self.pcols[2]], right_on=['nro_devolucion',self.pcols[2]])
            if df4.empty ==False: 
                df5 = self.ica.get_equalvalue(df4, 'descripcion6', 'anulado', 'ANU', 'Registro anulado')
                comment =  'Cantidad de las NCs de un F3 > cantidad del F3'
                if df5.empty == False:
                    df6 = self.ica.get_diffqty_pro(df5, self.pcols[4], 'cantidad',self.fcols[5], 'nro_devolucion' ,comment)
                    iokf3 = df6[self.pcols[0]].values
                    self.ica.update_db(iokf3,'GCO', 'OKK')
                    self.ica.update_db(iokf3,'Comentario GCO', 'Coincidencia exacta F3+UPC+QTY')
                    df7 = df6[df6['descripcion6']=='Confirmado']
                    df8= self.ica.get_diffvalue(df7, 'aaaa anulacion', yyyy, 'NAA', f'Registro con año de confirmación diferente a {yyyy}')

    def f3_verify_20(self, f3, status, yyyy):
        df1 = self.db[(self.db[self.pcols[1]]==status) & (self.db['source']!='B6')]
        df2= self.ica.get_fnan( df1, self.fcols[0], 'F3')
        if df2.empty == False: 
            df3 = self.ica.get_duplicates( df2,[self.fcols[5], self.pcols[2], self.pcols[4]], 'Cod Aut + UPC + Qty')
            ne = self.ica.get_notfound( df3, f3, [self.fcols[0],self.pcols[2]], ['nro_devolucion',self.pcols[2]], 'nro_devolucion', 'F3|UPC|QTY')
            df4 = pd.merge(df3, f3, left_on=[self.fcols[0],self.pcols[2]], right_on=['nro_devolucion',self.pcols[2]])
            if df4.empty ==False: 
                df5 = self.ica.get_equalvalue(df4, 'descripcion6', 'anulado', 'ANU', 'Registro anulado')
                comment =  'Cantidad de las NCs de un F3 > cantidad del F3'
                if df5.empty == False:
                    df6 = self.ica.get_diffqty_pro(df5, self.pcols[4], 'cantidad',self.fcols[5], 'nro_devolucion' ,comment)
                    iokf3 = df6[self.pcols[0]].values
                    self.ica.update_db(iokf3,'GCO', 'OKK')
                    self.ica.update_db(iokf3,'Comentario GCO', 'Coincidencia exacta F3+UPC+QTY')
                    df7 = df6[df6['descripcion6']=='Confirmado']
                    df8= self.ica.get_diffvalue(df7, 'aaaa anulacion', yyyy, 'NAA', f'Registro con año de confirmación diferente a {yyyy}')

    def f4_verify(self, f4, status, yyyy):
        df1 = self.db[(self.db[self.pcols[1]]==status) & (self.db[self.pcols[5]]=='cerrado')]
        df2 = self.ica.get_fnan( df1, self.fcols[1], 'F4')
        if df2.empty == False:
            df3 = self.ica.get_duplicates( df2, [self.fcols[5], 'local_trx',self.pcols[2], self.pcols[4]], 'Cod Aut + Local + UPC + Qty')
            ne = self.ica.get_notfound( df3, f4, [self.fcols[1], self.pcols[2]], ['nro_red_inventario','upc'], 'nro_red_inventario', 'F4|UPC|QTY')
            df4 = pd.merge(df3, f4, left_on=[self.fcols[1], self.pcols[2]], right_on=['nro_red_inventario','upc'])
            if df4.empty ==False: 
                df5 = self.ica.get_equalvalue(df4, 'estado', 'anulado', 'ANU', 'Registro anulado')
                df6 = self.ica.get_diffvalue(df5, 'aa creacion', yyyy, 'NAA', f'Registro con año de creación diferente a {yyyy}')
                comment = 'Cantidad de las NCs de un F4 > cantidad del F4'
                if df6.empty == False:
                    df7 = self.ica.get_diffqty_pro(df6, self.pcols[4], 'cantidad',self.fcols[5],'nro_red_inventario', comment)
                    iokf4 = df7[self.pcols[0]].values
                    self.ica.update_db(iokf4,'GCO', 'OKK')
                    self.ica.update_db(iokf4,'Comentario GCO', 'Coincidencia exacta F4+UPC+QTY')

    def f4_verify_20(self, f4, status, yyyy):
        df1 = self.db[(self.db[self.pcols[1]]==status)  & (self.db['source']!='B6')]
        df2 = self.ica.get_fnan( df1, self.fcols[1], 'F4')
        if df2.empty == False:
            df3 = self.ica.get_duplicates( df2, [self.fcols[5], self.pcols[2], self.pcols[4]], 'Cod Aut + UPC + Qty')
            ne = self.ica.get_notfound( df3, f4, [self.fcols[1], self.pcols[2]], ['nro_red_inventario','upc'], 'nro_red_inventario', 'F4|UPC|QTY')
            df4 = pd.merge(df3, f4, left_on=[self.fcols[1], self.pcols[2]], right_on=['nro_red_inventario','upc'])
            if df4.empty ==False: 
                df5 = self.ica.get_equalvalue(df4, 'estado', 'anulado', 'ANU', 'Registro anulado')
                df6 = self.ica.get_diffvalue(df5, 'aa creacion', yyyy, 'NAA', f'Registro con año de creación diferente a {yyyy}')
                comment = 'Cantidad de las NCs de un F4 > cantidad del F4'
                if df6.empty == False:
                    df7 = self.ica.get_diffqty_pro(df6, self.pcols[4], 'cantidad',self.fcols[5],'nro_red_inventario', comment)
                    iokf4 = df7[self.pcols[0]].values
                    self.ica.update_db(iokf4,'GCO', 'OKK')
                    self.ica.update_db(iokf4,'Comentario GCO', 'Coincidencia exacta F4+UPC+QTY')

    def f4_verify_20_b6(self, f4, status, yyyy):
        df1 = self.db[(self.db[self.pcols[1]]==status) & (self.db['source']=='B6')]
        df2 = self.ica.get_fnan( df1, self.fcols[1], 'F4')
        if df2.empty == False:
            df3 = self.ica.get_duplicates( df2, [self.fcols[5], 'sku', self.pcols[4]], 'Cod Aut + SKU + Qty')
            ne = self.ica.get_notfound( df3, f4, [self.fcols[1], 'sku'], ['nro_red_inventario','nro_producto'], 'nro_red_inventario', 'F4|UPC|QTY')
            df4 = pd.merge(df3, f4, left_on=[self.fcols[1], 'sku'], right_on=['nro_red_inventario','nro_producto'])
            if df4.empty ==False: 
                df5 = self.ica.get_equalvalue(df4, 'estado', 'anulado', 'ANU', 'Registro anulado')
                df6 = self.ica.get_diffvalue(df5, 'aa creacion', yyyy, 'NAA', f'Registro con año de creación diferente a {yyyy}')
                comment = 'Cantidad de las NCs de un F4 > cantidad del F4'
                if df6.empty == False:
                    df7 = self.ica.get_diffqty_pro(df6, self.pcols[4], 'cantidad',self.fcols[5],'nro_red_inventario', comment)
                    iokf4 = df7[self.pcols[0]].values
                    self.ica.update_db(iokf4,'GCO', 'OKK')
                    self.ica.update_db(iokf4,'Comentario GCO', 'Coincidencia exacta F4+UPC+QTY')

    def f5_verify(self, f5, status, yyyy):
        df1 = self.db[(self.db[self.pcols[1]]==status) & (self.db[self.pcols[5]]=='cerrado')]
        df2 = self.ica.get_fnan( df1, self.fcols[2], 'F5')
        if df2.empty ==False: 
            df3 = self.ica.get_duplicates( df2, [self.fcols[5], 'local_trx',self.pcols[2], self.pcols[4]], 'Cod Aut + Local + UPC + Qty')
            ne = self.ica.get_notfound( df3, f5, [self.fcols[2], self.pcols[2]], ['transfer','upc'], 'transfer', 'F5|UPC|Qty')
            df4 = pd.merge(df3, f5, left_on=[self.fcols[2], self.pcols[2]], right_on=['transfer','upc'])
            if df4.empty ==False: 
                df5 = self.ica.get_diffvalue(df4, 'estado', 'recibido', 'NRE', 'Registro con estado diferente a recibido')
                df6 = self.ica.get_equalvalue(df5, 'motivo_discrepancia', 'f5 no recibido', 'MDI', 'Registro con motivo de disc: F5 no recibido')
                df7 = self.ica.get_diffvalue(df6, 'aaaa reserva', yyyy, 'NAA', f'Registro con año de reserva diferente a {yyyy}')
                comment = 'Cantidad de las NCs de un F5 != (cantidad pickeada | cantidad recibida del F5)'
                if df7.empty == False: 
                    df8 = self.ica.get_diffqty_pro_f5(df7,  self.pcols[4], 'cant_recibida', self.fcols[5], 'transfer', comment)
                    iokf5 = df8[self.pcols[0]].values
                    self.ica.update_db(iokf5, 'GCO','OKK')
                    self.ica.update_db(iokf5, 'Comentario GCO', 'Coincidencia exacta F5+UPC+QTY')

    def f5_verify_20(self, f5, status, yyyy):
        df1 = self.db[(self.db[self.pcols[1]]==status) & (self.db['source']!='B6')]
        df2 = self.ica.get_fnan( df1, self.fcols[2], 'F5')
        if df2.empty ==False: 
            df3 = self.ica.get_duplicates( df2, [self.fcols[5], self.pcols[2], self.pcols[4]], 'Cod Aut + UPC + Qty')
            ne = self.ica.get_notfound( df3, f5, [self.fcols[2], self.pcols[2]], ['transfer','upc'], 'transfer', 'F5|UPC|Qty')
            df4 = pd.merge(df3, f5, left_on=[self.fcols[2], self.pcols[2]], right_on=['transfer','upc'])
            if df4.empty ==False: 
                df5 = self.ica.get_diffvalue(df4, 'estado', 'recibido', 'NRE', 'Registro con estado diferente a recibido')
                df6 = self.ica.get_equalvalue(df5, 'motivo_discrepancia', 'f5 no recibido', 'MDI', 'Registro con motivo de disc: F5 no recibido')
                df7 = self.ica.get_diffvalue(df6, 'aaaa reserva', yyyy, 'NAA', f'Registro con año de reserva diferente a {yyyy}')
                comment = 'Cantidad de las NCs de un F5 != (cantidad pickeada | cantidad recibida del F5)'
                if df7.empty == False: 
                    df8 = self.ica.get_diffqty_pro_f5(df7,  self.pcols[4], 'cant_recibida', self.fcols[5], 'transfer', comment)
                    iokf5 = df8[self.pcols[0]].values
                    self.ica.update_db(iokf5, 'GCO','OKK')
                    self.ica.update_db(iokf5, 'Comentario GCO', 'Coincidencia exacta F5+UPC+QTY')

    def f5_verify_20_b6(self, f5, status, yyyy):
        df1 = self.db[(self.db[self.pcols[1]]==status) &(self.db['source']=='B6') ]
        df2 = self.ica.get_fnan( df1, self.fcols[2], 'F5')
        if df2.empty ==False: 
            df3 = self.ica.get_duplicates( df2, [self.fcols[5], 'sku', self.pcols[4]], 'Cod Aut + SKU + Qty')
            ne = self.ica.get_notfound( df3, f5, [self.fcols[2], 'sku'], ['transfer','sku'], 'transfer', 'F5|SKU|Qty')
            df4 = pd.merge(df3, f5, left_on=[self.fcols[2], 'sku'], right_on=['transfer','sku'])
            if df4.empty ==False: 
                df5 = self.ica.get_diffvalue(df4, 'estado', 'recibido', 'NRE', 'Registro con estado diferente a recibido')
                df6 = self.ica.get_equalvalue(df5, 'motivo_discrepancia', 'f5 no recibido', 'MDI', 'Registro con motivo de disc: F5 no recibido')
                df7 = self.ica.get_diffvalue(df6, 'aaaa reserva', yyyy, 'NAA', f'Registro con año de reserva diferente a {yyyy}')
                comment = 'Cantidad de las NCs de un F5 != (cantidad pickeada | cantidad recibida del F5)'
                if df7.empty == False: 
                    df8 = self.ica.get_diffqty_pro_f5(df7,  self.pcols[4], 'cant_recibida', self.fcols[5], 'transfer', comment)
                    iokf5 = df8[self.pcols[0]].values
                    self.ica.update_db(iokf5, 'GCO','OKK')
                    self.ica.update_db(iokf5, 'Comentario GCO', 'Coincidencia exacta F5+UPC+QTY')

    def f5_verify_local(self, f5, status, yyyy, local):
        df1 = self.db[(self.db[self.pcols[1]]==status) & (self.db[self.pcols[5]]=='cerrado')]
        df2 = self.ica.get_fnan( df1, self.fcols[2], 'F5')
        if df2.empty ==False: 
            df3 = self.ica.get_duplicates( df2, [self.fcols[5], 'local_trx',self.pcols[2], self.pcols[4]], 'Cod Aut + Local + UPC + Qty')
            ne = self.ica.get_notfound( df3, f5, [self.fcols[2], self.pcols[2]], ['transfer','upc'], 'transfer', 'F5|UPC|Qty')
            df4 = pd.merge(df3, f5, left_on=[self.fcols[2], self.pcols[2]], right_on=['transfer','upc'])
            if df4.empty ==False:
                df5 = self.ica.get_diffvalue(df4, 'estado', 'recibido', 'NRE', 'Registro con estado diferente a recibido')
                df6 = self.ica.get_equalvalue(df5, 'motivo_discrepancia', 'f5 no recibido', 'MDI', 'Registro con motivo de disc: F5 no recibido')
                df7 = self.ica.get_diffvalue(df6, 'aaaa reserva', yyyy, 'NAA', f'Registro con año de reserva diferente a {yyyy}')
                comment = 'Cantidad de las NCs de un F5 != (cantidad pickeada | cantidad recibida del F5)'
                if df7.empty == False: 
                    df8 = self.ica.get_diffqty_pro_f5(df7,  self.pcols[4], 'cant_recibida', self.fcols[5], 'transfer', comment)
                    df9 = self.ica.get_diffvalue(df8, 'local_recep', local, 'NCL', f'Registro con local diferente a {local}')
                    iokf5 = df9[self.pcols[0]].values
                    self.ica.update_db(iokf5, 'GCO','OKK')
                    self.ica.update_db(iokf5, 'Comentario GCO', 'Coincidencia exacta F5+UPC+QTY')
    
    def f5_verify_local_list(self, f5, status, yyyy, local , locales):
        nil = []
        df1 = self.db[(self.db[self.pcols[1]]==status) & (self.db[self.pcols[5]]=='cerrado')]
        df2 = self.ica.get_fnan( df1, self.fcols[2], 'F5')
        if df2.empty ==False: 
            df3 = self.ica.get_duplicates( df2, [self.fcols[5], 'local_trx',self.pcols[2], self.pcols[4]], 'Cod Aut + Local + UPC + Qty')
            ne = self.ica.get_notfound( df3, f5, [self.fcols[2], self.pcols[2]], ['transfer','upc'], 'transfer', 'F5|UPC|Qty')
            df4 = pd.merge(df3, f5, left_on=[self.fcols[2], self.pcols[2]], right_on=['transfer','upc'])
            if df4.empty ==False: 
                df5 = self.ica.get_diffvalue(df4, 'estado', 'recibido', 'NRE', 'Registro con estado diferente a recibido')
                df6 = self.ica.get_equalvalue(df5, 'motivo_discrepancia', 'f5 no recibido', 'MDI', 'Registro con motivo de disc: F5 no recibido')
                df7 = self.ica.get_diffvalue(df6, 'aaaa reserva', yyyy, 'NAA', f'Registro con año de reserva diferente a {yyyy}')
                comment = 'Cantidad de las NCs de un F5 != (cantidad pickeada | cantidad recibida del F5)'
                df8, lista = self.ica.get_notinlist(df7, 'local_recep', locales, 'NCL', f'Registro con local diferente a {local}')
                if df8.empty == False: 
                    df9 = self.ica.get_diffqty_pro_f5(df7,  self.pcols[4], 'cant_recibida', self.fcols[5], 'transfer', comment)
                    iokf5 = df9[self.pcols[0]].values
                    self.ica.update_db(iokf5, 'GCO','OKK')
                    self.ica.update_db(iokf5, 'Comentario GCO', 'Coincidencia exacta F5+UPC+QTY')
                return lista 
            else:
                return nil 
        else: 
            return nil 
    
    def no_carga_verify(self, status):
        df1 = self.db[(self.db[self.pcols[1]]==status) & (self.db[self.pcols[5]]=='cerrado')]
        if df1.empty ==False: 
            df2 = self.ica.get_duplicates( df1, [self.fcols[5], 'local_trx',self.pcols[2], self.pcols[4]], 'Cod Aut + Local + UPC + Qty')
            df3 = self.ica.get_diffvalue(df2, 'local_ant', '3000', 'NCL', 'Local de venta != local 3000')
            ioknc = df3[self.pcols[0]].values
            self.ica.update_db(ioknc, 'GCO', 'OKK')
            self.ica.update_db(ioknc, 'Comentario GCO', 'Local de venta 3000')

    
    def refact_verify_20(self, refact):
        df1 = self.db[(self.db['source']=='B1') ]
        df2= self.ica.get_fnan( df1, self.fcols[4], 'F12')
        df3 = self.ica.get_duplicates( df2, [self.fcols[5], 'sku', self.pcols[4]], 'Cod Aut + SKU + Qty')
        ne = self.ica.get_notfound( df3, refact, [self.fcols[4]], ['f12cod'], 'f12cod', 'F12')
        df4 = pd.merge(df3, refact, left_on=[self.fcols[4]], right_on=['f12cod'])
        df5 = self.ica.get_equalvalue(df4, 'confirmacion_tesoreria', 'no reintegrado  trx declinada', 'ANU', 'Registro con TRX declinada')
        # df5 = cierres.ica.get_diffvalue(df4, 'estado', 'APPROVED', 'ANU', 'Registro con transacción anulada')
        # df6 = cierres.ica.get_diffqty_pro(df5, 'qproducto', 'cantidad',f11_col, f3_col,'La cantidad sumada de los f11s de un f3 es mayor que la cantidad del f3')
        iokf12 = df5[self.pcols[0]].values
        self.ica.update_db(iokf12,'GCO', 'OKK')
        self.ica.update_db(iokf12,'Comentario GCO', 'Coincidencia exacta F12')
        #self.ica.get_okk_dup(iokf12, 'Comentario GCO', 'F12')
        #self.ica.get_dup_i(iokf12, 'F12')

    def kpi_verify_20(self, kpi, yyyy, commenty):
        df1 = self.db[(self.db['source']=='B7') ]
        df2= self.ica.get_fnan_cols(df1, [self.fcols[4],self.fcols[3]], 'KPID')
        if df2.empty == False:
            df3 = self.ica.get_duplicates( df2, [self.fcols[5], 'sku', self.pcols[4]], 'Cod Aut + SKU + Qty')
            index_ne_kpi_di = self.ica.get_notfound( df3, kpi, [self.fcols[3]], ['entrada'], 'entrada', '(F12|F11)')
            index_ne_kpi_di2 = self.ica.get_notfound( self.db.loc[index_ne_kpi_di], kpi, [self.fcols[4]], ['entrada'], 'entrada', '(F12|F11)')
            pgdim1 = pd.merge(df3, kpi, left_on=[self.fcols[3]], right_on=['entrada'])
            pgdim2 = pd.merge(df3.loc[index_ne_kpi_di], kpi, left_on=[self.fcols[4]], right_on=['entrada'])
            lpgdi = [pgdim1, pgdim2]
            pgdim = pd.concat(lpgdi, axis=0)
            pgdimdyear = '' 
            if yyyy == '2021': 
                pgdimdyear = self.ica.get_lvalue(pgdim, 'fecha_paletiza', pd.Timestamp(2021,1,21), 'NAA',commenty)
            elif yyyy =='2020':
                pgdimdyear = self.ica.get_gvalue(pgdim, 'fecha_paletiza', pd.Timestamp(2021,1,21), 'NAA', commenty)
            iokkpid = pgdimdyear[self.pcols[0]].values
            self.ica.update_db(iokkpid,'GCO', 'OKK')
            self.ica.update_db(iokkpid,'Comentario GCO', 'Coincidencia exacta (F12|F11)')
            #self.ica.get_okk_dup(iokkpid, 'Comentario GCO', '(F12|F11)')
            #self.ica.get_dup_i(iokkpid, '(F12|F11)')

    def kpi_verify_20_2435(self, kpi, yyyy, commenty):
        df1 = self.db[(self.db['tipmc']=='recibido en cd') & ((self.db['source']=='B2')|(self.db['source']=='B3')|(self.db['source']=='B4')|(self.db['source']=='B5'))]
        df2= self.ica.get_fnan_cols(df1, [self.fcols[4],self.fcols[3]], 'KPID')
        if df2.empty == False:
            df3 = self.ica.get_duplicates( df2, [self.fcols[5], 'sku', self.pcols[4]], 'Cod Aut + SKU + Qty')
            index_ne_kpi_di = self.ica.get_notfound( df3, kpi, [self.fcols[3]], ['entrada'], 'entrada', '(F12|F11)')
            index_ne_kpi_di2 = self.ica.get_notfound( self.db.loc[index_ne_kpi_di], kpi, [self.fcols[4]], ['entrada'], 'entrada', '(F12|F11)')
            pgdim1 = pd.merge(df3, kpi, left_on=[self.fcols[3]], right_on=['entrada'])
            pgdim2 = pd.merge(df3.loc[index_ne_kpi_di], kpi, left_on=[self.fcols[4]], right_on=['entrada'])
            lpgdi = [pgdim1, pgdim2]
            pgdim = pd.concat(lpgdi, axis=0)
            pgdimdyear = '' 
            if yyyy == '2021': 
                pgdimdyear = self.ica.get_lvalue(pgdim, 'fecha_paletiza', pd.Timestamp(2021,1,21), 'NAA',commenty)
            elif yyyy =='2020':
                pgdimdyear = self.ica.get_gvalue(pgdim, 'fecha_paletiza', pd.Timestamp(2021,1,21), 'NAA', commenty)
            iokkpid = pgdimdyear[self.pcols[0]].values
            self.ica.update_db(iokkpid,'GCO', 'OKK')
            self.ica.update_db(iokkpid,'Comentario GCO', 'Coincidencia exacta (F12|F11)')
            #self.ica.get_okk_dup(iokkpid, 'Comentario GCO', '(F12|F11)')
            #self.ica.get_dup_i(iokkpid, '(F12|F11)')
    
    def starting(self, cols):
        # verificar duplicidad en toda la base 
        self.ica.get_dup_all_db(cols)
        
    def finals(self):
        # verificar registros revisados
        self.ica.get_checked()