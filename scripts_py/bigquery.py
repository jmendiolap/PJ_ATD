#CREDENCIALES A BQ


from google.cloud import bigquery
from google.oauth2 import service_account
import time
import sys
from tqdm import trange

def up_bq(bq_modo,resumen,resumen_x):
    project_id = 'pe-pjp-cld-01'
    tabla = 'resumentest'
    tabla1 = 'resumenxtest'
    credentials = service_account.Credentials.from_service_account_file(
        r'C:\Users\opaucarb\PycharmProjects\a_trans_digital\credenciales\bq_python.json',
    )
    def do_something():
        time.sleep(0.4)

    def do_another_something():
        time.sleep(0.4)

    for i in trange(1, file=sys.stdout, desc='-CARGA DE ARCHIVOS BIGQUERY'):
        do_something()

        for i in trange(1, file=sys.stdout, desc='--RESUMENES '):
            errors0 = resumen.to_gbq(destination_table='PY_DATA_TRANS_DIGITAL.'+tabla,
                                       project_id='pe-pjp-cld-01',
                                       if_exists=bq_modo,
                                       credentials=credentials)
            errors0 = resumen_x.to_gbq(destination_table='PY_DATA_TRANS_DIGITAL.'+tabla1,
                                       project_id='pe-pjp-cld-01',
                                       if_exists=bq_modo,
                                       credentials=credentials)

            do_something()
