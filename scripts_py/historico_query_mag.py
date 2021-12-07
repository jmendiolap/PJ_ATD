import pandas as pd
import cx_Oracle #importar libreria
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas_gbq
import os
import scripts_py.leer_sql as leer_sql
import datetime
#se ejecuta solo una vez al iniciar el SO
cx_Oracle.init_oracle_client(lib_dir=r"C:\Users\opaucarb\PycharmProjects\PJ_EPresupuestal\instantclient_21_3")
#script propios

sql_consulta=r"C:\Users\opaucarb\PycharmProjects\a_trans_digital\SQL_script\query_magistrado_historico.txt"
#consulta=leer_sql.leer(sql_consulta)

#coneccion a DB
dsn = cx_Oracle.makedsn(host='172.34.0.120', port=7520, sid='DBPRUEBAS ') #configurar dns
conn = cx_Oracle.connect(user='ADMINPJ', password='789456', dsn=dsn)#usario que se conectara a la base de datos

#intervalo de fechas
start = datetime.datetime.strptime("16/11/2021", "%d/%m/%Y")
end = datetime.datetime.strptime("02/12/2021", "%d/%m/%Y")
date_generated = [start + datetime.timedelta(days=x) for x in range(0, (end-start).days)]

for date in date_generated:
    fecha = date.strftime("%d/%m/%Y")
    sql1 = 'SELECT DISTINCT '
    consulta_fin = leer_sql.leer(sql_consulta)
    consulta = sql1 + "'"+ fecha +"'"+' '+ consulta_fin
    frame = pd.read_sql(consulta, conn)
    fecha2 = fecha.replace('/', '-')
    frame.to_excel('query_magistrado_'+fecha2+'.xlsx')

