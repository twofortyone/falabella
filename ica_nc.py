import pandas as pd
from datetime import datetime
import numpy as np

class InternalControlAnalysis:

    def __init__(self, db, indexcol, costcol, statuscol, qtycol, upccol) -> None:
        """ 
        Get rows with different quantity 
        :param bd: dataframe base
        """
        self.db = db.copy()
        self.index_column = indexcol
        self.cost_column = costcol 
        self.status_column = statuscol
        self.qty_column = qtycol
        self.upc_column = upccol
        self.dt_string = datetime.now().strftime('%y%m%d-%H%M%S')
        self.fcols = ['f3col-0', 'f4col-1', 'f5col-2', 'f11col-3', 'f12col-4', 'cod-nc-5']
    
    def set_fcols(self, cols):
        self.fcols = cols

    def get_db(self):
        return self.db
    
    def update_db(self, rwos, cols, value):
        self.db.loc[rwos, cols] = value
     
    def get_duplicates(self, bdquery, cols, llave):
        """
        Get duplicates
        :param bdquery: dataframe to identify duplicates
        :param cols: (list) columns to identify duplicates
        :param numf: (string)
        """
        du = bdquery[bdquery.duplicated(cols, keep=False)]
        idu = du[self.index_column].values
        self.db.loc[idu, 'GCO'] = 'DUP'
        self.db.loc[idu, 'Comentario GCO'] = f'Registro duplicado {llave}'
        bdquery_res = bdquery[~bdquery.duplicated(cols, keep=False)]
        return bdquery_res


    def get_fnan(self, bdquery, col, numf):
        """
        Get rows with nan in F value
        :param bdquery: dataframe to identify duplicates
        :param col: (string)
        :param numf: (string)
        """
        fnan = bdquery[bdquery[col].isna()]
        #fnan.to_csv(f'output/{self.dt_string}-fnan.csv', sep=';', decimal=',', index=False) 
        inf5 = fnan[self.index_column].values
        self.db.loc[inf5, 'GCO'] = 'N' + numf
        self.db.loc[inf5, 'Comentario GCO'] = f'Registro sin número de {numf}'
        bdquery_res = bdquery[bdquery[col].notna()]
        return bdquery_res
    
    def get_fnan_cols(self, bdquery, cols, numf):
        """
        Get rows with nan in F value
        :param bdquery: dataframe to identify duplicates
        :param col: (string)
        :param numf: (string)
        """
        fnan = bdquery[bdquery[cols].isna().all(1)]
        #fnan.to_csv(f'output/{self.dt_string}-fnan.csv', sep=';', decimal=',', index=False) 
        inf5 = fnan[self.index_column].values
        self.db.loc[inf5, 'GCO'] = 'N' + numf
        self.db.loc[inf5, 'Comentario GCO'] = 'No existe número de ' + numf
        bdquery_res = bdquery[bdquery[cols].notna().any(1)]
        return bdquery_res

    def get_notfound(self, bdquery, dff, leftcols, rightcols, col, llave):
        """
        Get not found rows
        :param bdquery: dataframe to identify not found rows 
        :param dff: (dataframe) Fs dataframe 
        :param leftcols: (list)
        :param rightcols: (list)
        :param col: (string)
        :param numf: (string)
        """
        lmerge = bdquery.merge(dff, how='left',left_on=leftcols, right_on=rightcols)
        ne = lmerge[lmerge[col].isna()]
        #ne.to_csv(f'output/{self.dt_string}-ne-{numf}.csv', sep=';', decimal=',', index=False) 
        ine = ne[self.index_column].values
        self.db.loc[ine, 'GCO'] = 'NFD'
        self.db.loc[ine, 'Comentario GCO'] = f'No coincide {llave}'
        return ine
    
    def get_diffqty(self, bdquery, qty1col, qty2col, numf):
        """ 
        Get rows with different quantity
        :param bdquery: dataframe to identify different quantity
        :param qty1col: (string) quantity 1 
        :param qty2col: (string) quantity 2 
        :param numf: (string) 
        """
        dc = bdquery[bdquery[qty1col]!=bdquery[qty2col]] # Registros con cantidades diferentes
        idc = dc[self.index_column].values
        self.db.loc[idc, 'GCO' ] = 'NCC'
        bdquery_res = bdquery[bdquery[qty1col]==bdquery[qty2col]]
        return bdquery_res
    
    def get_diffqty_pro(self, bdquery, qty1col, qty2col, fl, ff, comment):
        """ 
        Get rows with different quantity
        :param bdquery: dataframe to identify different quantity
        :param qty1col: (string) quantity 1 
        :param qty2col: (string) quantity 2 
        """
        s = bdquery[[fl, ff,qty1col, qty2col]].groupby([fl, ff]).sum().reset_index()
        fs = list(s[s[qty1col]>s[qty2col]][fl].values)
        dc = bdquery[bdquery[fl].isin(fs)] # Registros con cantidades diferentes
        idc = dc[self.index_column].values
        self.db.loc[idc, 'GCO' ] = 'NCC'
        self.db.loc[idc, 'Comentario GCO' ] = comment
        bdquery_res = bdquery[~bdquery[fl].isin(fs)]
        return bdquery_res


    def get_canceledstatus(self, bdquery, statuscol):
        """ 
        Get rows with different quantity
        :param bdquery: dataframe to identify canceled status rows 
        :param statuscol: (string) status column 
        :param numf: (string) 
        """
        anu = bdquery[bdquery[statuscol]=='Anulado']
        ianu = anu[self.index_column].values 
        self.db.loc[ianu, 'GCO'] = 'ANU'
        self.db.loc[ianu, 'Comentario GCO'] = 'Registro anulado'
        bdquery_res = bdquery[bdquery[statuscol]!='Anulado']
        return  bdquery_res

    def get_diffyear(self, bdquery, yearcol, year):
        """ 
        Get rows with different year 
        :param bdquery: dataframe to identify canceled status rows 
        :param yearcol: (string) year column 
        :param year: (string) year of comparison 
        """
        diffyear = bdquery[bdquery[yearcol]!=year]
        idiffyear = diffyear[self.index_column].values
        self.db.loc[idiffyear, 'GCO'] = 'NAA'
        bdquery_res = bdquery[bdquery[yearcol]==year]
        return bdquery_res

    def get_diffvalue(self, bdquery, valuecol, value, note, comment):
        """ 
        Get rows with different value 
        :param bdquery: dataframe to query
        :param valuecol: (string) value column 
        :param value: (string) value of comparison 
        """
        diffvalue = bdquery[bdquery[valuecol]!=value]
        idiffvalue = diffvalue[self.index_column].values
        self.db.loc[idiffvalue, 'GCO'] = note
        self.db.loc[idiffvalue, 'Comentario GCO'] = comment
        bdquery_res = bdquery[bdquery[valuecol]==value]
        return bdquery_res

    def get_equalvalue(self, bdquery, valuecol, value, note, comment):
        """ 
        Get rows with different value 
        :param bdquery: dataframe to query
        :param valuecol: (string) value column 
        :param value: (string) value of comparison 
        """
        equalvalue = bdquery[bdquery[valuecol]==value]
        iequalvalue = equalvalue[self.index_column].values
        self.db.loc[iequalvalue, 'GCO'] = note
        self.db.loc[iequalvalue, 'Comentario GCO'] = comment
        bdquery_res = bdquery[bdquery[valuecol]!=value]
        return bdquery_res
    
    def get_notinlist(self, bdquery, valuecol, list, note, comment):
        notinlist = bdquery[~bdquery[valuecol].isin(list)]
        inotinlist = notinlist[self.index_column].values
        self.db.loc[inotinlist, 'GCO'] = note
        self.db.loc[inotinlist, 'Comentario GCO'] = comment
        bdquery_res = bdquery[bdquery[valuecol].isin(list)]
        return bdquery_res

    def get_menorvalue(self, bdquery, valuecol, value, note, comment):
        """ 
        Get rows with different value 
        :param bdquery: dataframe to query
        :param valuecol: (string) value column 
        :param value: (string) value of comparison 
        """
        equalvalue = bdquery[bdquery[valuecol]>value]
        imvalue = equalvalue[self.index_column].values
        self.db.loc[imvalue, 'GCO'] = note
        self.db.loc[imvalue, 'Comentario GCO'] = comment
        bdquery_res = bdquery[bdquery[valuecol]<=value]
        return bdquery_res

    def f4_verify(self, f4, status, yyyy):
        df1 = self.db[(self.db[self.status_column]==status) & (self.db['esmc']=='CERRADO')]
        df2 = self.get_fnan( df1, self.fcols[1], 'F4')
        if df2.empty == False:
            df3 = self.get_duplicates( df2, ['cod_aut_nc', 'local_trx','upc', 'cantidad_trx_actual'], 'Cod Aut + Local + UPC + Qty')
            ne = self.get_notfound( df3, f4, [self.fcols[1], self.upc_column], ['nro. red. inventario','upc'], 'nro. red. inventario', 'F4|UPC|QTY')
            df4 = pd.merge(df3, f4, left_on=[self.fcols[1], self.upc_column], right_on=['nro. red. inventario','upc'])
            if df4.empty ==False: 
                df5 = self.get_equalvalue(df4, 'estado_y', 'Anulado', 'ANU', 'Registro anulado')
                df6 = self.get_diffvalue(df5, 'aa creacion', yyyy, 'NAA', f'Registro con año de creación diferente a {yyyy}')
                comment = 'La cantidad sumada de los cod. aut. nc de un F4 es mayor que la cantidad del F4'
                df7 = self.get_diffqty_pro(df6, self.qty_column, 'cantidad',self.fcols[5],'nro. red. inventario', comment)
                iokf4 = df7[self.index_column].values
                self.update_db(iokf4,'GCO', 'OKK')
                self.update_db(iokf4,'Comentario GCO', 'Coincidencia exacta F4+UPC+QTY')

    def f5_verify(self, f5, status, yyyy, dqpcol):
        df1 = self.db[(self.db[self.status_column]==status) & (self.db['esmc']=='CERRADO')]
        df2 = self.get_fnan( df1, self.fcols[2], 'F5')
        if df2.empty ==False: 
            df3 = self.get_duplicates( df2, ['cod_aut_nc', 'local_trx','upc', 'cantidad_trx_actual'], 'Cod Aut + Local + UPC + Qty')
            ne = self.get_notfound( df3, f5, [self.fcols[2], self.upc_column], ['transfer','upc'], 'transfer', 'F5|UPC|Qty')
            df4 = pd.merge(df3, f5, left_on=[self.fcols[2], self.upc_column], right_on=['transfer','upc'])
            if df4.empty ==False: 
                df5 = self.get_diffvalue(df4, 'estado_y', 'Recibido', 'NRE', 'Registro con estado diferente a recibido')
                df6 = self.get_equalvalue(df5, 'motivo discrepancia', 'F5 NO RECIBIDO', 'MDI', 'Registro con motivo de disc: F5 no recibido')
                df7 = self.get_diffvalue(df6, 'aaaa reserva', yyyy, 'NAA', f'Registro con año de reserva diferente a {yyyy}')
                comment = f'La cantidad sumada de los {dqpcol} de un F5 es mayor que la cantidad del F5'
                df8 = self.get_diffqty_pro(df7,  self.qty_column, 'cant. recibida', dqpcol, 'transfer', comment)
                iokf5 = df8[self.index_column].values
                self.update_db(iokf5, 'GCO','OKK')
                self.update_db(iokf5, 'Comentario GCO', 'Coincidencia exacta F5+UPC+QTY')

    def f5_verify_local(self, f5, status, yyyy, dqpcol, local):
        df1 = self.db[(self.db[self.status_column]==status) & (self.db['esmc']=='CERRADO')]
        df2 = self.get_fnan( df1, self.fcols[2], 'F5')
        if df2.empty ==False: 
            df3 = self.get_duplicates( df2, ['cod_aut_nc', 'local_trx','upc', 'cantidad_trx_actual'], 'Cod Aut + Local + UPC + Qty')
            ne = self.get_notfound( df3, f5, [self.fcols[2], self.upc_column], ['transfer','upc'], 'transfer', 'F5|UPC|Qty')
            df4 = pd.merge(df3, f5, left_on=[self.fcols[2], self.upc_column], right_on=['transfer','upc'])
            if df4.empty ==False: 
                df5 = self.get_diffvalue(df4, 'estado_y', 'Recibido', 'NRE', 'Registro con estado diferente a recibido')
                df6 = self.get_equalvalue(df5, 'motivo discrepancia', 'F5 NO RECIBIDO', 'MDI', 'Registro con motivo de disc: F5 no recibido')
                df7 = self.get_diffvalue(df6, 'aaaa reserva', yyyy, 'NAA', f'Registro con año de reserva diferente a {yyyy}')
                comment = f'La cantidad sumada de los {dqpcol} de un F5 es mayor que la cantidad del F5'
                df8 = self.get_diffqty_pro(df7,  self.qty_column, 'cant. recibida', dqpcol, 'transfer', comment)
                df9 = self.get_diffvalue(df8, 'local recep', local, 'NCL', f'Registro con local diferente a {local}')
                iokf5 = df9[self.index_column].values
                self.update_db(iokf5, 'GCO','OKK')
                self.update_db(iokf5, 'Comentario GCO', 'Coincidencia exacta F5+UPC+QTY')
    
    def f5_verify_local_list(self, f5, status, yyyy, dqpcol, locales):
        df1 = self.db[(self.db[self.status_column]==status) & (self.db['esmc']=='CERRADO')]
        df2 = self.get_fnan( df1, self.fcols[2], 'F5')
        if df2.empty ==False: 
            df3 = self.get_duplicates( df2, ['cod_aut_nc', 'local_trx','upc', 'cantidad_trx_actual'], 'Cod Aut + Local + UPC + Qty')
            ne = self.get_notfound( df3, f5, [self.fcols[2], self.upc_column], ['transfer','upc'], 'transfer', 'F5|UPC|Qty')
            df4 = pd.merge(df3, f5, left_on=[self.fcols[2], self.upc_column], right_on=['transfer','upc'])
            if df4.empty ==False: 
                df5 = self.get_diffvalue(df4, 'estado_y', 'Recibido', 'NRE', 'Registro con estado diferente a recibido')
                df6 = self.get_equalvalue(df5, 'motivo discrepancia', 'F5 NO RECIBIDO', 'MDI', 'Registro con motivo de disc: F5 no recibido')
                df7 = self.get_diffvalue(df6, 'aaaa reserva', yyyy, 'NAA', f'Registro con año de reserva diferente a {yyyy}')
                comment = f'La cantidad sumada de los {dqpcol} de un F5 es mayor que la cantidad del F5'
                df8 = self.get_diffqty_pro(df7,  self.qty_column, 'cant. recibida', dqpcol, 'transfer', comment)
                df9 = self.get_notinlist(df8, 'local recep', locales, 'NCL', f'Registro con local diferente a {locales}')
                iokf5 = df9[self.index_column].values
                self.update_db(iokf5, 'GCO','OKK')
                self.update_db(iokf5, 'Comentario GCO', 'Coincidencia exacta F5+UPC+QTY')

    def f3_verify(self, f3, status, yyyy, dqpcol):
        df1 = self.db[(self.db[self.status_column]==status) & (self.db['esmc']=='CERRADO')]
        df2= self.get_fnan( df1, self.fcols[0], 'F3')
        if df2.empty == False: 
            df3 = self.get_duplicates( df2,['cod_aut_nc', 'local_trx','upc', 'cantidad_trx_actual'], 'Cod Aut + Local + UPC + Qty')
            ne = self.get_notfound( df3, f3, [self.fcols[0],self.upc_column], ['nro devolucion','upc'], 'nro devolucion', 'F3|UPC|QTY')
            df4 = pd.merge(df3, f3, left_on=[self.fcols[0],self.upc_column], right_on=['nro devolucion','upc'])
            if df4.empty ==False: 
                df5 = self.get_equalvalue(df4, 'descripcion.6', 'Anulado', 'ANU', 'Registro anulado')
                comment = f'La cantidad sumada de los {dqpcol} de un f3 es mayor que la cantidad del f3'
                df6 = self.get_diffqty_pro(df5, self.qty_column, 'cantidad',dqpcol, 'nro devolucion' ,comment)
                iokf3 = df6[self.index_column].values
                self.update_db(iokf3,'GCO', 'OKK')
                self.update_db(iokf3,'Comentario GCO', 'Coincidencia exacta F3+UPC+QTY')
                df7 = df6[df6['descripcion.6']=='Confirmado']
                df8= self.get_diffvalue(df7, 'aaaa anulacion', yyyy, 'NAA', f'Registro con año de confirmación diferente a {yyyy}')
