# @title LIMPIEZA Y TRATAMIENDO DE DATOS DE RESOLUCIONES
from datetime import datetime
import os
import pandas as pd
import import_ipynb
import datetime
import scripts_py.unir_mismo as unir_mismo
from datetime import datetime


def limpieza(ruta, df_magistrados_maestra,fecha2):
    df_resoluciones = pd.read_excel(ruta)
    df_resoluciones = (df_resoluciones.loc[:, [
                                                  'corte',
                                                  'n_unico',
                                                  'n_incidente',
                                                  'c_perfil',
                                                  'desper',
                                                  'c_usuario',
                                                  'x_nom_usuario',
                                                  'c_dni',
                                                  'l_ind_estado',
                                                  'f_descargo',
                                                  'f_registro'

                                              ]
                       ])

    # df_resoluciones = df_resoluciones.drop_duplicates(subset=['c_dni'])
    df_resoluciones.columns = [
        'CORTE',
        'N_UNICO',
        'N_INCIDENTE',
        'C_PERFIL',
        'PERFIL',
        'C_USUARIO',
        'NOMBRE_MAGISTRADO',
        'DNI_MAGISTRADO',
        'l_ind_estado',
        'f_descargo',
        'FECHA'
    ]
    #
    # Cuenta la cantidad de DNIs
    df_resoluciones_cant_dni = df_resoluciones.groupby(['DNI_MAGISTRADO']).count()
    df_resoluciones_cant_dni = df_resoluciones_cant_dni.iloc[:, [0]]
    df_resoluciones_cant_dni = df_resoluciones_cant_dni.rename(columns={'CORTE': 'CANTIDAD_RESO'})
    # reseteando el index
    df_resoluciones_cant_dni = df_resoluciones_cant_dni.reset_index()
    # se quita los duplicados
    df_resoluciones = df_resoluciones.drop_duplicates(subset=['DNI_MAGISTRADO'])
    # agregando cantidades de sentido de fallo
    df_resoluciones_cant = df_resoluciones.merge(
        df_resoluciones_cant_dni,
        how='outer',
        indicator='Comparacion_cant',
        left_on=
        ['DNI_MAGISTRADO'
         ]
        , right_on=
        ['DNI_MAGISTRADO'
         ]
    )
    # COMPARACION SESIONES VS MAGISTRADOS
    maestra_vs_resoluciones = df_magistrados_maestra.merge(
        df_resoluciones_cant,
        how='outer',
        indicator='Comparacion',
        left_on=
        ['DNI_MAGISTRADO'
         ]
        , right_on=
        ['DNI_MAGISTRADO'
         ]
    )

    maestra_vs_resoluciones['FECHA'] = pd.to_datetime(maestra_vs_resoluciones['FECHA'])
    a = maestra_vs_resoluciones['FECHA'].unique().tolist()
    a = pd.to_datetime(a, format='%Y/%m/%d')
    a = a.strftime('%Y/%d/%m')
    maestra_vs_resoluciones = maestra_vs_resoluciones.assign(FECHA_global=fecha2)
    maestra_vs_resoluciones

    return maestra_vs_resoluciones