# Librerías
import pandas as pd
from datetime import datetime
from cl_cleaning import CleaningText as ct 
from tqdm import tqdm

dt_string = datetime.now().strftime('%y%m%d-%H%M')

# Configuraciones
pd.set_option('float_format', '{:,.2f}'.format)

config = open('input/get_data_config.txt', 'r', encoding='ISO-8859-1')
gdlines = [line.strip() for line in config.readlines()]
config.close()

class GetData():

    def __init__(self) -> None:
        self.lista = []
        self.names = ['f3', 'f4', 'f5', 'kpi','refact']
        self.dfs_colsreq = []
        self.lista_fnum = []
        self.lista_num = []
        self.lista_text = []

        # Inicializar variables 
        self.set_colsreq()
        self.set_colsnum()
        self.set_colstext()
    
    def set_colsreq(self):
        # Declaración de columnas requeridas 
        f3_colsreq = ['nro_devolucion', 'fecha_reserva', 'fecha_envio', 'fecha_anulacion', 'fecha_confirmacion', 'upc', 'sku', 'linea', 'descripcion6', 'cantidad', 'folio_f11', 'folio_f12']
        f4_colsreq = ['nro_red_inventario', 'estado','fecha_creacion', 'destino', 'linea','upc', 'cantidad','f11']
        f5_colsreq = ['transfer', 'estado', 'fe_reserva', 'fe_envo', 'fe_recep','local_envo', 'local_recep', 'tipo_de_f5','sku', 'upc', 'cant_pickeada', 'cant_recibida', 'motivo_discrepancia']
        kpi_colsreq = ['index', 'tip0_trabajo', 'entrada','fecha_paletiza', 'aaaa_paletiza']
        refac_colsreq = ['medio_pago','cod#aut', '4_ult', 'f12cod', 'orden_de_compra','cedula', 'valor_boleta','fecha_devolucion', 'confirmacion_facturacion', 'confirmacion_tesoreria']
        self.dfs_colsreq = [f3_colsreq , f4_colsreq , f5_colsreq , kpi_colsreq , refac_colsreq]

    def set_colsnum(self):
        # Columnas con datos númericos 
        # Números de Fs, upcs, sku
        f3_fnum = ['nro_devolucion','upc', 'sku','folio_f11', 'folio_f12']
        f4_fnum = ['nro_red_inventario', 'upc', 'f11']
        f5_fnum = ['transfer', 'sku', 'upc']
        kpi_fnum = ['entrada']
        refact_fnum = ['cod#aut', '4_ult', 'f12cod', 'orden_de_compra','cedula']

        # Costos y cantidades 
        f3_num = ['cantidad']
        f4_num = ['cantidad']
        f5_num = ['cant_pickeada', 'cant_recibida']

        self.lista_fnum= [f3_fnum, f4_fnum, f5_fnum, kpi_fnum, refact_fnum]
        self.lista_num= [f3_num, f4_num, f5_num,'kpi', 'refac']

    def set_colstext(self):
        # Texto 
        f3_text = ['linea', 'descripcion6']
        f4_text = ['estado','destino', 'linea']
        f5_text = ['estado', 'tipo_de_f5', 'motivo_discrepancia']
        kpi_text = ['tip0_trabajo']
        refact_text = ['medio_pago','confirmacion_facturacion', 'confirmacion_tesoreria']
        self.lista_text = [f3_text, f4_text, f5_text, kpi_text, refact_text]

    def load_data(self, f3_dir, f4_dir, f5_dir, kpi_dir, refact_dir, db_dir):
        # Cargar data
        f3 = pd.read_csv(f3_dir, sep=';', dtype='object')
        f4 = pd.read_csv(f4_dir, sep=';', dtype='object')
        f5 = pd.read_csv(f5_dir, sep=';', dtype='object')
        kpi = pd.read_csv(kpi_dir, sep=';', dtype='object')
        refact = pd.read_csv(refact_dir, sep=';', dtype='object')
        db = pd.read_csv(db_dir, sep=';', dtype='object')

        # Inicializar estructuras según tipo análisis
        self.lista =[f3, f4, f5, kpi, refact, db]

    def get_data(self):
        # Normailzar headers
        print('Normalizando encabezados')
        for item in tqdm(self.lista): 
            ct.norm_header(item)

        # Eliminar columnas no requeridas
        def drop_except(df, cols):
            df.drop(df.columns.difference(cols), axis=1, inplace=True)
            return df 

        print('Eliminando columnas no requeridas')
        for i in tqdm(range(len(self.lista))): 
            drop_except(self.lista[i],self.dfs_colsreq[i])

        # Limpiar texto
        print('Limpiando texto en columnas')
        for i, item in enumerate(tqdm(self.lista_text)):
            self.lista[i].loc[:, item] = self.lista[i].loc[:, item].apply(ct.clean_str)

        # Convertir a número fs 
        print('Convirtiendo a número parte 1')
        for i, item in enumerate(tqdm(self.lista_fnum)):
            self.lista[i].loc[:, item] = self.lista[i].loc[:, item].apply(ct.clean_fnum)

        # Convertir a número cantidades y costos 
        print('Convirtiendo a número parte 2')
        for i, item in enumerate(tqdm(self.lista_num)):
            if (i!=3)&(i!=4): 
                self.lista[i].loc[:, item] = self.lista[i].loc[:, item].apply(ct.clean_num)

        # Eliminar filas duplicados 
        self.lista[0].drop_duplicates(['nro_devolucion', 'upc'], inplace= True)
        self.lista[1].drop_duplicates(['nro_red_inventario', 'upc'], inplace=True)
        self.lista[2].drop_duplicates(['transfer','upc'], inplace=True)
        self.lista[3].drop_duplicates(['entrada'], inplace=True)
        self.lista[4].drop_duplicates(['f12cod', 'orden_de_compra'], inplace=True)

        # Eliminar registros con #s de F nulos 
        self.lista[0] = self.lista[0][self.lista[0].nro_devolucion.notna()]
        self.lista[1] = self.lista[1][self.lista[1].nro_red_inventario.notna()]
        self.lista[2] = self.lista[2][self.lista[2].transfer.notna()]
        self.lista[3] = self.lista[3][self.lista[3].entrada.notna()]
        self.lista[4] = self.lista[4][self.lista[4].f12cod.notna()]

    def save_files(self, folder):
        # Guardar archivos 
        print('Guardando archivos')
        for i in tqdm(range(len(self.lista))):
            path = f'input/{folder}/{dt_string}-{self.names[i]}.csv'
            self.lista[i].to_csv(path, sep=';', index=False, encoding='utf-8') 
            print(path)

    def update_lists(self, name, colsreq, fnum, num, text):
        #self.lista.append(bd)
        self.names.append(name)
        self.dfs_colsreq.append(colsreq)
        self.lista_fnum.append(fnum)
        self.lista_num.append(num)
        self.lista_text.append(text)
    
    def run_gd(self, data_select):
        # Data aggregation 
        if data_select=='1': # CF11s CD 2020 
            cf11_20_colsreq  = ['nfolio','f12', 'prd_upc', 'qproducto', 'xobservacion', 'total_costo_promedio', 'estado_actual', 'status_nuevo', 'f3nuevo', 'f4_nuevo', 'nuevo_f11', 'f5', 'reporte_a_contabilidad', 'movimiento_contable', 'nc', 'tranf_electro_factura', 'pv'] # Para cd 2020 
            cf11_20_fnum = ['nfolio','f12', 'prd_upc', 'f3nuevo', 'f4_nuevo', 'nuevo_f11', 'f5']
            cf11_20_num = [ 'qproducto', 'total_costo_promedio']
            cf11_20_text = ['xobservacion','estado_actual', 'status_nuevo']
            self.update_lists('cf11_cd_20', cf11_20_colsreq, cf11_20_fnum, cf11_20_num, cf11_20_text)
            self.get_data()
            self.save_files('cierres_f11/cd')

        elif data_select=='2': # CF11s 2021 
            cf11_21_colsreq  = ['f11','f12', 'prd_upc', 'qproducto', 'xobservacion', 'costo_total', 'estado_actual', 'status_final', 'f3', 'f4', 'f5'] # Para cd 2021 
            cf11_21_fnum = ['f11','f12', 'prd_upc', 'f3', 'f4', 'f5']
            cf11_21_num = [ 'qproducto', 'costo_total'] 
            cf11_21_text = ['xobservacion','estado', 'status_final']
            self.update_lists('cf11_cd_21', cf11_21_colsreq, cf11_21_fnum, cf11_21_num, cf11_21_text)
            self.get_data()
            self.save_files('cierres_f11/cd')

        elif data_select =='3': # CF11s Tienda 2020 
            cf11_tienda_colsreq = ['nfolio','prd_upc', 'estado', 'producto', 'qproducto', 'total_costo_promedio', 'f', 'motivo']
            cf11_tienda_fnum = ['nfolio', 'prd_upc', 'f']
            cf11_tienda_num = [ 'qproducto', 'total_costo_promedio'] 
            cf11_tienda_text = ['motivo']
            self.update_lists('cf11_tienda_20', cf11_tienda_colsreq, cf11_tienda_fnum, cf11_tienda_num, cf11_tienda_text)
            self.get_data()
            self.save_files('cierres_f11/tienda')

        elif data_select == '4': # Cierres NCs 
            cnc_colsreq = ['cod_aut_nc', 'local_trx', 'terminal', 'local_ant', 'upc', 'ct', 'cantidad_trx_actual', 'tipo_nc', 'f3', 'f4','f5', 'f11', 'esmc', 'tipmc']        
            cnc_fnum = ['cod_aut_nc','local_trx', 'terminal', 'local_ant', 'upc', 'f3', 'f4', 'f5', 'f11']
            cnc_num = [ 'ct', 'cantidad_trx_actual'] 
            cnc_text = ['esmc', 'tipmc']
            self.update_lists('cierres_nc', cnc_colsreq, cnc_fnum, cnc_num, cnc_text)
            self.get_data()
            self.save_files('cierres_nc')
        else: 
            print('Seleccione una opción correcta (1-4)')
    
def menu():
    print('------------------    Procesar datos')
    print('1. Cierres de F11s CD auditoria')
    print('2. Cierres de F11s CD 2021')
    print('3. Cierres de F11s Tienda')
    print('4. Cierres de NCs')
    return input('Seleccione una opción (1-4):')

def init_commandline():
    gd = GetData()
    selection_var = menu()
    gd.load_data(gdlines[0], gdlines[1], gdlines[2], gdlines[3], gdlines[4], gdlines[5])
    gd.run_gd(selection_var)

if __name__=='__main__':
    init_commandline()