# LIMPIEZA PARA SENTIDO DE fallos
import pandas as pd
import numpy as np
def limpieza(ruta, df_magistrados_maestra,fecha):
    df_fallos = pd.read_excel(ruta)
    df_fallos = (df_fallos.loc[:, [
                                      'c_dj',
                                      'x_dj',
                                      'n_aaaamm',
                                      'n_dia',
                                      'n_sentidoFallo',
                                      'f_sentidoFallo',
                                      'f_publicaciÃ³n',
                                      'id_juzgado',
                                      'x_juzgado',
                                      'x_sentidoFallo',
                                      'x_formato',
                                      'x_dni',
                                      'x_magistrado',
                                  ]
                 ])

    df_fallos.columns = [
        'C_CORTE',
        'CORTE',
        'FECHA_MA',
        'FECHA_DIA',
        'N_SENTIDO_FALLO',
        'FECHA_SF',
        'FECHA',
        'ID_JUZGADO',
        'JUZGADO',
        'SF_DETALLE',
        'X_FORMATO',
        'DNI_MAGISTRADO',
        'NOMBRE_MAGISTRADO',
    ]
    # Cuenta la cantidad de DNIs
    df_fallos_cant_dni = df_fallos.groupby(['DNI_MAGISTRADO']).count()
    df_fallos_cant_dni = df_fallos_cant_dni.iloc[:, [0]]
    df_fallos_cant_dni = df_fallos_cant_dni.rename(columns={'C_CORTE': 'CANTIDAD_FALLO'})
    # reseteando el index
    df_fallos_cant_dni = df_fallos_cant_dni.reset_index()

    # se quita los duplicados
    df_fallos = df_fallos.drop_duplicates(subset=['DNI_MAGISTRADO'])
    # agregando cantidades de sentido de fallo
    df_fallos_cant = df_fallos.merge(
        df_fallos_cant_dni,
        how='outer',
        indicator='Comparacion_cant',
        left_on=
        ['DNI_MAGISTRADO'
         ]
        , right_on=
        ['DNI_MAGISTRADO'
         ]
    )

    maestra_vs_fallos = df_magistrados_maestra.merge(
        df_fallos_cant,
        how='outer',
        indicator='Comparacion',
        left_on=
        ['DNI_MAGISTRADO'
         ]
        , right_on=
        ['DNI_MAGISTRADO'
         ]
    )
    maestra_vs_fallos['FECHA']  = maestra_vs_fallos['FECHA'].astype(str)
    maestra_vs_fallos['FECHA']  = maestra_vs_fallos['FECHA'] .apply(lambda x: x[:10])

    a = maestra_vs_fallos['FECHA'].unique().tolist()
    a = [item.replace("-", "/") for item in a]

    maestra_vs_fallos = maestra_vs_fallos.assign(FECHA_global=fecha)

    n_registros_maestra_vs_fallos = maestra_vs_fallos.shape[0]
    maestra_vs_fallos['N_REGISTROS'] = n_registros_maestra_vs_fallos
    maestra_vs_fallos['DNI_MAGISTRADO'] = maestra_vs_fallos['DNI_MAGISTRADO'].astype(str)
    maestra_vs_fallos['DNI_MAGISTRADO'] = maestra_vs_fallos['DNI_MAGISTRADO'].apply(lambda x: x.zfill(8))
    maestra_vs_fallos[['COD_UNIDAD_EJECUTORA', 'COD_CORTE','COD_TIPO_DEPENDENCIA','COD_DEPENDENCIA']]= maestra_vs_fallos[['COD_UNIDAD_EJECUTORA', 'COD_CORTE','COD_TIPO_DEPENDENCIA','COD_DEPENDENCIA']].astype(float)
    return maestra_vs_fallos
#return maestra_vs_fallos

