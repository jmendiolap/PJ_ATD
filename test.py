import pandas as pd
import cx_Oracle
# importar libreria
from google.oauth2 import service_account
import pandas_gbq
import leer_sql
import os
from google.cloud import bigquery
# se ejecuta solo una vez al iniciar el SO
cx_Oracle.init_oracle_client(lib_dir=r"D:\OMAR\instaladores\instantclient_21_3")

# script propios

df = pd.DataFrame()
df['first_name'] = ['Josy', 'Vaughn', 'Neale', 'Teirtza']
df['last_name'] = ['Clarae', 'Halegarth', 'Georgievski', 'Teirtza']
df['gender'] = ['Female', 'Male', 'Male', 'Female']

# CREDENCIALES A BQ
credentials = service_account.Credentials.from_service_account_file(
    'D:/OMAR/REPORTES/credenciales_bq/bq_python.json',
)

# INSERCCION DE DATA A BQ
# 'fail'
# Si existe una tabla, genere pandas_gbq.gbq.TableCreationError.
# 'replace'
# Si la tabla existe, suéltela, vuelva a crearla e inserte los datos.
# 'append'
# Si existe una tabla, inserte los datos. Crear si no existe.

df.to_gbq(destination_table='python.test221',
          project_id='pe-pjp-cld-01',
          if_exists='append',
          credentials=credentials)

if errors == [None]:
    print('New rows have been added.')
else:
    print(f'Encountered errors while inserting rows: {errors}')