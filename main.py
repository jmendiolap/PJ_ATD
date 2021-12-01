# LIBRERIAS
import os
import pandas as pd
import numpy as np
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas_gbq
import time
import sys
from tqdm import trange
import random
# ----------------------------------
import scripts_py.limp_maestra as limp
# import scripts_py.unir_mismo
import scripts_py.limp_sesiones as sesi
# import limp_marcaciones as marc
import scripts_py.limp_resoluciones as reso
import scripts_py.limp_notificaciones as noti
import scripts_py.limp_fallos as fall
import scripts_py.limp_jescucha as jesc
import scripts_py.resumenes as resumen

import scripts_py.bigquery as bigq

# import limp_casillero as casi

def do_something():
    time.sleep(0.3)
# title RUTA DEL DIRECTORIO
# **********************************************************************************************************************
fecha               = "19-11-2021"
ruta                = "C:/Users/opaucarb/Documents/AVANCE_TRANSFORMACION_DIGITAL/ARCHIVOS/"
ruta_credencial_bq  = r'C:\Users\opaucarb\PycharmProjects\a_trans_digital\credenciales\bq_python.json'
directorio = ruta + fecha
lista_archivos=os.listdir(directorio)
# verificar que los archivos correspondan
#-----------------------------------------------------------------------------------------------------------------------
a_maestra            = directorio + '\\' +"Query_Magistrados_V3.xlsx"
a_sesiones_asa       = directorio + '\\' +"01.Script_GLPI_xxxxx_Reporte_Sesiones_Juez_x_dia ASA V3.xlsx"
a_sesiones_ase       = directorio + '\\' +"01.Script_GLPI_xxxxx_Reporte_Sesiones_Juez_x_dia ASE V3.xlsx"
a_resoluciones       = directorio + '\\' +"02.Resoluciones_Firmada-Modificada V3.xlsx"
a_notificaciones_asa = directorio + '\\' +"03.Notificaciones_Juez_x_dia ASA V3.xlsx"
a_notificaciones_ase = directorio + '\\' +"03.Notificaciones_Juez_x_dia ASE V3.xlsx"
a_fallos             = directorio + '\\' +"Sentido de Fallo Diario V3.xlsx"
#a_jescucha           = directorio + '\\' +"JUEZ ESCUCHA-"+fecha+".xlsx"
# a_casillero        = directorio + '\\' +""
#-----------------------------------------------------------------------------------------------------------------------
# 'fail' # Si existe una tabla, genere pandas_gbq.gbq.TableCreationError.
# 'replace' # Si la tabla existe, suéltela, vuelva a crearla e inserte los datos.
# 'append' # Si existe una tabla, inserte los datos. Crear si no existe.

bq_modo                     = 'replace'
project_id                  = 'pe-pjp-cld-01'
contenedor                  = 'PY_DATA_TRANS_DIGITAL2'
bq_t_resumen                = 'resumen'
bq_t_resumen_x              = 'resumen_X'
bq_maestra_vs_sesiones      = 'maestra_vs_sesiones'
bq_maestra_vs_resoluciones  = 'maestra_vs_resoluciones'
bq_maestra_vs_notificaciones= 'maestra_vs_notificaciones'
bq_maestra_vs_fallos        = 'maestra_vs_fallos'
#bq_maestra_vs_jescuha       = 'maestra_vs_juez_escucha'
#***********************************************************************************************************************
#limpieza de dataos

for i in trange(1, file=sys.stdout, desc='******************** LECTURA Y LIMPIEZA DE ARCHIVOS ********************'):
    do_something()
    df_maestra                = limp.limpieza(a_maestra)
    for i in trange(random.randint(1, 5), file=sys.stdout, desc='-----MAESTRA '):
        do_something()

    maestra_vs_sesiones       = sesi.limpieza(a_sesiones_asa,a_sesiones_ase,df_maestra)
    for i in trange(random.randint(1, 5), file=sys.stdout, desc='-----MAESTRA VS SESIONES '):
        do_something()

    maestra_vs_resoluciones   = reso.limpieza(a_resoluciones,df_maestra)
    for i in trange(random.randint(1, 5), file=sys.stdout, desc='-----MAESTRA VS RESOLUCIONES '):
        do_something()

    maestra_vs_notificaciones = noti.limpieza(a_notificaciones_asa,a_notificaciones_ase,df_maestra)
    for i in trange(random.randint(1, 5), file=sys.stdout, desc='-----MAESTRA VS NOTIFICACIONES '):
        do_something()

    maestra_vs_fallos         = fall.limpieza(a_fallos,df_maestra)
    for i in trange(random.randint(1, 5), file=sys.stdout, desc='-----MAESTRA VS FALLOS '):
        do_something()

    #maestra_vs_jescucha       = jesc.limpieza(a_jescucha,df_maestra)
    #for i in trange(random.randint(1, 5), file=sys.stdout, desc='-----MAESTRA VS JUEZ ESCUCHA'):
    #    do_something()

    #resumenes   = resumen.resumenes(maestra_vs_sesiones, maestra_vs_resoluciones, maestra_vs_notificaciones, maestra_vs_fallos, maestra_vs_jescucha)
    resumenes   = resumen.resumenes(maestra_vs_sesiones, maestra_vs_resoluciones, maestra_vs_notificaciones, maestra_vs_fallos)
    for i in trange(random.randint(1, 5), file=sys.stdout, desc='-----RESUMENES'):
        do_something()

#-----------------------------------------------------------------------------------------------------------------------
#RESUMENES

#-----------------------------------------------------------------------------------------------------------------------
#ENVIO A BIGQUERY
#INSERCCION DE DATA A BQ

credentials = service_account.Credentials.from_service_account_file(ruta_credencial_bq,)
contenedor  = contenedor + "."
print("\033[1;34m"+"-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+")

for i in trange(1, file=sys.stdout, desc='******************** CARGA DE ARCHIVOS BIGQUERY ********************'):
    do_something()
    print("\n")
    errors0 = resumenes[0].to_gbq(destination_table=contenedor + bq_t_resumen,
                                  project_id='pe-pjp-cld-01',
                                  if_exists=bq_modo,
                                  credentials=credentials)
    print("\033[1;31m" + bq_t_resumen +"--------------------- Carga satisfactoria a BigQuery--OK!")
#-----------------------------------------------------------------------------------------------------------------------
    errors0 = resumenes[1].to_gbq(destination_table=contenedor + bq_t_resumen_x,
                                  project_id='pe-pjp-cld-01',
                                  if_exists=bq_modo,
                                  credentials=credentials)
    print("\033[1;31m" + bq_t_resumen_x +"------------------- Carga satisfactoria a BigQuery--OK!")
# -----------------------------------------------------------------------------------------------------------------------
    errors0 = maestra_vs_sesiones.to_gbq(destination_table=contenedor + bq_maestra_vs_sesiones,
                                  project_id='pe-pjp-cld-01',
                                  if_exists=bq_modo,
                                  credentials=credentials)
    print("\033[1;31m" + bq_maestra_vs_sesiones +"--------- Carga satisfactoria a BigQuery--OK!")
# -----------------------------------------------------------------------------------------------------------------------
    errors0 = maestra_vs_resoluciones.to_gbq(destination_table=contenedor + bq_maestra_vs_resoluciones,
                                  project_id='pe-pjp-cld-01',
                                  if_exists=bq_modo,
                                  credentials=credentials)
    print("\033[1;31m" + bq_maestra_vs_resoluciones +"----- Carga satisfactoria a BigQuery--OK!")
# -----------------------------------------------------------------------------------------------------------------------
    errors0 = maestra_vs_notificaciones.to_gbq(destination_table=contenedor + bq_maestra_vs_notificaciones,
                                  project_id='pe-pjp-cld-01',
                                  if_exists=bq_modo,
                                  credentials=credentials)
    print("\033[1;31m" + bq_maestra_vs_notificaciones +"--- Carga satisfactoria a BigQuery--OK!")
# -----------------------------------------------------------------------------------------------------------------------
    errors0 = maestra_vs_fallos.to_gbq(destination_table=contenedor + bq_maestra_vs_fallos,
                                  project_id='pe-pjp-cld-01',
                                  if_exists=bq_modo,
                                  credentials=credentials)
    print("\033[1;31m" + bq_maestra_vs_fallos +"----------- Carga satisfactoria a BigQuery--OK!")
# -----------------------------------------------------------------------------------------------------------------------
    #errors0 = maestra_vs_jescucha.to_gbq(destination_table=contenedor + bq_maestra_vs_jescuha,
    #                              project_id='pe-pjp-cld-01',
    #                              if_exists=bq_modo,
    #                              credentials=credentials)
    #print("\033[1;31m" + bq_maestra_vs_jescuha +"----------- Carga satisfactoria a BigQuery--OK!")
# -----------------------------------------------------------------------------------------------------------------------

print("\033[1;33m"+"PROCESO COMPLETADO CON EXITO XD - VE Y COMETE UN TOKTOCHI"+'\033[0;m')