import pandas as pd
from datetime import datetime
import numpy as np 

class InternalControlAnalysis:

    def __init__(self, db, indexcol) -> None:
        """ 
        Get rows with different quantity 
        :param bd: dataframe base
        """
        self.db = db.copy()
        self.index_column = indexcol
        self.dt_string = datetime.now().strftime('%y%m%d-%H%M')

    def get_db(self):
        return self.db
    
    def update_db(self, rwos, cols, value):
        self.db.loc[rwos, cols] = value
    
    def get_okk_dup(self, rwos, cols, llave):
        aux = self.db.loc[rwos]
        aux = aux.loc[(aux['GCO']=='OKK')&(aux['gco_dupall']=='y')]
        self.db.loc[aux[self.index_column].values, cols] = f'Coincidencia {llave} + Registro duplicado en DB'

    def get_dup_i(self, rwos, llave):
        # Actualizar datos de OKK sin validar duplicados 
        aux = self.db.loc[rwos]
        aux = aux.loc[(self.db['GCO']=='OKK') & (self.db['fnandup']=='y')]
        self.db.loc[aux[self.index_column], 'Comentario GCO'] = f'Coincidencia {llave} - Se requiere F12|UPC|QTY para validar duplicidad'

    def get_duplicates(self, bdquery, cols, llave):
        """
        Get duplicates
        :param bdquery: dataframe to identify duplicates
        :param cols: (list) columns to identify duplicates
        :param numf: (string)
        """
        fnandup = bdquery[(bdquery.duplicated(cols, keep=False))&(bdquery[cols].isna().any(axis=1))]
        ifnd = fnandup[self.index_column].values
        self.db.loc[ifnd, 'fnandup'] = 'y'
        du = bdquery[(bdquery.duplicated(cols, keep=False))& (bdquery[cols].notna().all(axis=1))]
        idu = du[self.index_column].values
        self.db.loc[idu, 'GCO'] = 'DUP'
        self.db.loc[idu, 'Comentario GCO'] = f'Registro duplicado {llave}'
        bdquery_res = bdquery[~bdquery.duplicated(cols, keep=False)]
        return pd.concat([bdquery_res,fnandup], axis=0)

    def get_dup_all_db(self,cols):
        # Verificar duplicidad en toda la base 
        self.db.loc[(self.db.duplicated(cols, keep=False)) &(self.db[cols].notna().all(axis=1)), 'gco_dup'] = 'y'
        self.db.loc[(self.db.duplicated(cols, keep=False)) &(self.db[cols].isna()).any(axis=1), 'gco_dup'] = 'i'
        self.db.loc[self.db['gco_dup'].isna(), 'gco_dup'] = 'n'

    def get_checked(self):
        # verificar registros revisados
        self.db.loc[self.db['GCO'].notna(), 'checked'] = 'y'
        self.db.loc[self.db['GCO'].isna(), 'checked'] = 'n'

    def get_ru_dup_mc(self):
        pass

    def get_dupmc(self, indexes):
        pass

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
        self.db.loc[inf5, 'Comentario GCO'] = f'Registro sin nro. de {numf}'
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
        self.db.loc[inf5, 'Comentario GCO'] = 'Registro sin nro. de ' + numf
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

    def get_diffqty_pro_f5(self, bdquery, qty1col, qty2col, fl, ff, comment):
        """ 
        Get rows with different quantity
        :param bdquery: dataframe to identify different quantity
        :param qty1col: (string) quantity 1 
        :param qty2col: (string) quantity 2 
        """
        s = bdquery[[fl, ff,qty1col, qty2col, 'cant_pickeada']].groupby([fl, ff]).sum().reset_index()
        fs = list(s[(s[qty1col]!= s[qty2col]) & (s[qty1col]!= s['cant_pickeada'])][fl].values)
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

    def get_gvalue(self, bdquery, valuecol, value, note, comment):
        """ 
        Get rows with different value 
        :param bdquery: dataframe to query
        :param valuecol: (string) value column 
        :param value: (string) value of comparison 
        """
        equalvalue = bdquery[bdquery[valuecol]>=value]
        imvalue = equalvalue[self.index_column].values
        self.db.loc[imvalue, 'GCO'] = note
        self.db.loc[imvalue, 'Comentario GCO'] = comment
        bdquery_res = bdquery[bdquery[valuecol]<value]
        return bdquery_res
    
    def get_lvalue(self, bdquery, valuecol, value, note, comment):
        """ 
        Get rows with different value 
        :param bdquery: dataframe to query
        :param valuecol: (string) value column 
        :param value: (string) value of comparison 
        """
        equalvalue = bdquery[bdquery[valuecol]<value]
        imvalue = equalvalue[self.index_column].values
        self.db.loc[imvalue, 'GCO'] = note
        self.db.loc[imvalue, 'Comentario GCO'] = comment
        bdquery_res = bdquery[bdquery[valuecol]>=value]
        return bdquery_res

    def get_notinlist(self, bdquery, valuecol, lista, note, comment):
        notinlist3 = bdquery[~bdquery[valuecol].isin(lista)]
        inotinlist = notinlist3[self.index_column].values
        self.db.loc[inotinlist, 'GCO'] = note
        self.db.loc[inotinlist, 'Comentario GCO'] = comment
        bdquery_res = bdquery[bdquery[valuecol].isin(lista)]
        return bdquery_res, list(notinlist3['local_recep'])
    
        #-----------------------------------------------------
    def dupall(self):
        # 30 de junio de 2021 
        # Comparar duplicados con los de michael 
        # TODO  pasar a método 
        dup_cols = ['f12', 'prd_upc']
        redcols = ['f12', 'prd_upc','status_nuevo']

        concept1 = 'cierre x duplicidad (f11 con mismo f12+sku+cantidad)'
        concept2 = 'registro duplicado en base de datos'
        sin_cat_dup = self.db[(self.db['status_nuevo']!= concept1)&(self.db['status_nuevo']!=concept2)]
        cat_dup = self.db[((self.db['status_nuevo']== concept1)|(self.db['status_nuevo']==concept2))]
        self.db.loc[((self.db['status_nuevo']== concept1)|(self.db['status_nuevo']==concept2)), 'checked'] ='y'
        #sin_cat_dup = self.db[self.db['status_nuevo']!= concept1] # No es categoría dup
        #cat_dup = self.db[self.db['status_nuevo']== concept1] # Es categoría dup

        cat_dup_mas_gco = cat_dup.loc[cat_dup['gco_dup']=='y'] # Duplicados para MC y GCO

        cat_dup_mas_gco = cat_dup_mas_gco[redcols]
        self.db.loc[cat_dup_mas_gco.index, 'dupmc'] = 'y'
        self.db.loc[cat_dup_mas_gco.index, 'checked'] = 'y'
        cat_dup_mas_gco.drop_duplicates(dup_cols, inplace=True)

        mdup = pd.merge(sin_cat_dup, cat_dup_mas_gco, on=dup_cols,validate='many_to_one') # Registros unicos MC de duplicados
        print(mdup.empty)
        self.db.loc[mdup['indice_cf11'].values, 'dupmc'] = 'y'
        self.db.loc[mdup['indice_cf11'].values, 'checked'] = 'y'

        aux = mdup[mdup.duplicated(dup_cols)]
        self.db.loc[aux['indice_cf11'].values, 'dupmc'] = np.nan
        self.db.loc[aux['indice_cf11'].values,'error_ru'] = 'y'

        self.db.loc[(self.db['dupmc'].isna())& (self.db['gco_dup'] =='y') ,'gco_dupall'] = 'y'
        #self.db.loc[(self.db['dupmc'].isna())& (self.db['gco_dup'] =='y') & (self.db.GCO =='OKK'),'Comentario GCO'] = 'Coincidencia exacta + Registro duplicado en DB'
    #-----------------------------------------------------