from io import SEEK_SET
from os import pipe
import os

select_var = ''

def clean():
    os.system('cls' if os.name=='nt' else 'clear')

def menu():
    print('-----------------------')
    print(' ### Menú inicial')
    print('1. Obtener data')
    print('2. Cierres de F11s')
    print('3. Cierres de NC')
    print('4. Salir')
    print('-----------------------')

def menu_cl():
    print('  1. Limpiar data')
    print('  2. Procesar data')
    print('  3. Regresar al menú')

def menu_cf11():
    print('  1. Cierres de F11 CD 2020')
    print('  2. Cierres de F11 CD 2021')
    print('  3. Cierres de F11 Tienda 2020')
    print('  4. Cierres de F11 Tienda 2021')
    print('  5. Regresar al menú')

def menu_cnc():
    print('  1. Cierres de NC Local 3000 - 2020')
    print('  2. Cierres de NC 2021')
    print('  3. Regresar al menú')

while select_var!='4':
    menu()
    select_var = input('Seleccione una tarea: ')
    
    if select_var =='1':
        menu_cl()
        sv_cl = input('  Rta: ')
        if sv_cl=='1':
            exec(open('cl_fs.py').read())
        elif sv_cl=='2':
            exec(open('get_data.py').read())
        elif sv_cl=='3':
            clean()
        else:
            print('    Por favor seleccione una opción valida!')

    elif select_var=='2':
        menu_cf11()
        sv_cf11= input('  Rta: ')
        if sv_cf11=='1':
            exec(open('cf11_cd_20.py').read())
        elif sv_cf11=='2':
            exec(open('cf11_cd_21.py').read())
        elif sv_cf11=='3':
            exec(open('cf11_tienda_20.py').read())
        elif sv_cf11=='4':
            exec(open('cf11_tienda_21.py').read())
        elif sv_cf11 =='5':
            clean()
        else:
            print('    Por favor seleccione una opción valida!')

    elif select_var=='3':
        menu_cnc()
        sv_nc= input('  Rta: ')
        if sv_nc=='1':
            exec(open('cierres_nc_cd_20.py').read())
        elif sv_nc=='2':
            exec(open('cierres_nc_cd.py').read())
        elif sv_nc=='3':
            clean()
        else:
            print('    Por favor seleccione una opción valida!')
        
    elif select_var=='4':
        print('# Hasta luego!')
    else:
        print('  Por favor seleccione una opción valida!')
