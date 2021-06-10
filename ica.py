import pandas as pd
from datetime import datetime
import numpy as np

class InternalControlAnalysis:

    def __init__(self, db, indexcol, costcol) -> None:
        """ 
        Get rows with different quantity 
        :param bd: dataframe base
        """
        self.db = db
        self.index_column = indexcol
        self.cost_column = costcol 
        self.dt_string = datetime.now().strftime('%y%m%d-%H%M%S')

    def get_db(self):
        return self.db
     
    def get_duplicates(self, bdquery, cols, numf):
        """
        Get duplicates
        :param bdquery: dataframe to identify duplicates
        :param cols: (list) columns to identify duplicates
        :param numf: (string)
        """
        du = bdquery[bdquery.duplicated(cols, keep=False)]
        idu = du[self.index_column].values
        self.db.loc[idu, 'GCO'] = 'DUPLICADO'
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
        bdquery_res = bdquery[bdquery[cols].notna().any(1)]
        return bdquery_res

    def get_notfound(self, bdquery, dff, leftcols, rightcols, col, numf):
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
        self.db.loc[ine, 'GCO'] = 'NFOUND'
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
        self.db.loc[idc, 'GCO' ] = 'NCCANT'
        bdquery_res = bdquery[bdquery[qty1col]==bdquery[qty2col]]
        return bdquery_res
    
    def get_diffqty_pro(self, bdquery, qty1col, qty2col, fl, ff):
        """ 
        Get rows with different quantity
        :param bdquery: dataframe to identify different quantity
        :param qty1col: (string) quantity 1 
        :param qty2col: (string) quantity 2 
        """
        s = bdquery[[fl, ff,qty1col, qty2col]].groupby([fl, ff]).sum().reset_index()
        fs = list(s[s.qproducto>s.cantidad][fl].values)
        dc = bdquery[bdquery[fl].isin(fs)] # Registros con cantidades diferentes
        idc = dc[self.index_column].values
        self.db.loc[idc, 'GCO' ] = 'NCCANT'
        bdquery_res = bdquery[~bdquery[fl].isin(fs)]
        return bdquery_res


    def get_canceledstatus(self, bdquery, statuscol, numf):
        """ 
        Get rows with different quantity
        :param bdquery: dataframe to identify canceled status rows 
        :param statuscol: (string) status column 
        :param numf: (string) 
        """
        anu = bdquery[bdquery[statuscol]=='Anulado']
        ianu = anu[self.index_column].values 
        self.db.loc[ianu, 'GCO'] = 'ANULADO'
        bdquery_res = bdquery[bdquery[statuscol]!='Anulado']
        return  bdquery_res

    def get_diffyear(self, bdquery, yearcol, year, numf):
        """ 
        Get rows with different year 
        :param bdquery: dataframe to identify canceled status rows 
        :param yearcol: (string) year column 
        :param year: (string) year of comparison 
        """
        diffyear = bdquery[bdquery[yearcol]!=year]
        idiffyear = diffyear[self.index_column].values
        self.db.loc[idiffyear, 'GCO'] = 'NANO'
        bdquery_res = bdquery[bdquery[yearcol]==year]
        return bdquery_res

    def get_diffvalue(self, bdquery, valuecol, value, numf, note):
        """ 
        Get rows with different value 
        :param bdquery: dataframe to query
        :param valuecol: (string) value column 
        :param value: (string) value of comparison 
        """
        diffvalue = bdquery[bdquery[valuecol]!=value]
        idiffvalue = diffvalue[self.index_column].values
        self.db.loc[idiffvalue, 'GCO'] = note
        bdquery_res = bdquery[bdquery[valuecol]==value]
        return bdquery_res

    def get_equalvalue(self, bdquery, valuecol, value, numf, note):
        """ 
        Get rows with different value 
        :param bdquery: dataframe to query
        :param valuecol: (string) value column 
        :param value: (string) value of comparison 
        """
        equalvalue = bdquery[bdquery[valuecol]==value]
        iequalvalue = equalvalue[self.index_column].values
        self.db.loc[iequalvalue, 'GCO'] = note
        bdquery_res = bdquery[bdquery[valuecol]!=value]
        return bdquery_res

    def get_menorvalue(self, bdquery, valuecol, value, numf, note):
        """ 
        Get rows with different value 
        :param bdquery: dataframe to query
        :param valuecol: (string) value column 
        :param value: (string) value of comparison 
        """
        equalvalue = bdquery[bdquery[valuecol]>value]
        iequalvalue = equalvalue[self.index_column].values
        self.db.loc[iequalvalue, 'GCO'] = note
        bdquery_res = bdquery[bdquery[valuecol]<=value]
        return bdquery_res