# LIMPIEZA PARA SENTIDO DE fallos
import pandas as pd
import numpy as np

def limpieza(ruta, df_magistrados_maestra,fecha):
    df_jescucha = pd.read_excel(ruta)
    df_jescucha = (df_jescucha.loc[:, [
                                      'ID_EVENTO',
                                      'FECHA_HORA_PROGRAMACION',
                                      'EXPEDIENTE',
                                      'ESPECIALIDAD',
                                      'DISTRITO_JUDICIAL',
                                      'ORGANO_JURISDICCIONAL',
                                      'FECHA_HORA_ATENCION',
                                      'DNI_USUARIO_ATENCION',
                                      'FECHA_OPERACION',
                                      'USUARIO_ATENCION',
                                      'PERFIL_USUARIO_ATENCION',
                                      'TIPO_CITA',
                                      'ESTADO',
                                      'F_ATENCION_INICIO',
                                      'F_ATENCION_FIN',
                                      'F_ATENCION_INICIO2'
                                  ]
                 ])

    df_jescucha.columns = [
                        'ID_EVENTO',
                        'FECHA_HORA_PROGRAMACION',
                        'EXPEDIENTE',
                        'ESPECIALIDAD',
                        'CORTE',
                        'ORGANO_JURISDICCIONAL',
                        'FECHA_HORA_ATENCION',
                        'DNI_MAGISTRADO',
                        'FECHA_OPERACION',
                        'NOMBRE_MAGISTRADO',
                        'PERFIL_USUARIO_ATENCION',
                        'TIPO_CITA',
                        'ESTADO',
                        'FECHA',
                        'F_ATENCION_FIN',
                        'F_ATENCION_INICIO2'
    ]
    # Cuenta la cantidad de DNIs
    df_jescucha_cant_dni = df_jescucha.groupby(['DNI_MAGISTRADO']).count()
    df_jescucha_cant_dni = df_jescucha_cant_dni.iloc[:, [0]]
    df_jescucha_cant_dni = df_jescucha_cant_dni.rename(columns={'C_CORTE': 'CANTIDAD_FALLO'})
    # reseteando el index
    df_jescucha_cant_dni = df_jescucha_cant_dni.reset_index()
    df_jescucha_cant_dni['DNI_MAGISTRADO']=df_jescucha_cant_dni['DNI_MAGISTRADO'].astype(str)
    # se quita los duplicados
    df_jescucha = df_jescucha.drop_duplicates(subset=['DNI_MAGISTRADO'])
    #df_jescucha['DNI_MAGISTRADO'] = df_jescucha['DNI_MAGISTRADO'].str.replace("[NULL]", "")
    #df_jescucha['DNI_MAGISTRADO'] = df_jescucha['DNI_MAGISTRADO'].astype(int)
    df_jescucha['DNI_MAGISTRADO'] = pd.to_numeric(df_jescucha['DNI_MAGISTRADO'],errors='coerce')
    df_jescucha['DNI_MAGISTRADO'] = df_jescucha['DNI_MAGISTRADO'].replace(np.nan, 0, regex=True)
    df_jescucha['DNI_MAGISTRADO'] = df_jescucha['DNI_MAGISTRADO'].astype(str)
    df_magistrados_maestra['DNI_MAGISTRADO'] = df_magistrados_maestra['DNI_MAGISTRADO'].astype(str)
    # agregando cantidades de sentido de fallo
    df_jescucha_cant = df_jescucha.merge(
        df_jescucha_cant_dni,
        how='outer',
        indicator='Comparacion_cant',
        left_on=
        ['DNI_MAGISTRADO'
         ]
        , right_on=
        ['DNI_MAGISTRADO'
         ]
    )

    maestra_vs_jescucha = df_magistrados_maestra.merge(
        df_jescucha_cant,
        how='outer',
        indicator='Comparacion',
        left_on=
        ['DNI_MAGISTRADO'
         ]
        , right_on=
        ['DNI_MAGISTRADO'
         ]
    )
    maestra_vs_jescucha['FECHA'] = pd.to_datetime(maestra_vs_jescucha['FECHA'])
    #a = maestra_vs_jescucha['FECHA'].unique().tolist()
    #a = pd.to_datetime(a, format='%d/%m/%Y')
    #a = a.strftime('%d/%m/%Y')


    maestra_vs_jescucha = maestra_vs_jescucha.assign(FECHA_global=fecha)
    maestra_vs_jescucha['FECHA'] = maestra_vs_jescucha['FECHA'].astype(str)

    n_registros_maestra_vs_jescucha = maestra_vs_jescucha.shape[0]
    maestra_vs_jescucha['N_REGISTROS'] = n_registros_maestra_vs_jescucha
    maestra_vs_jescucha['DNI_MAGISTRADO'] = maestra_vs_jescucha['DNI_MAGISTRADO'].astype(str)
    maestra_vs_jescucha['DNI_MAGISTRADO'] = maestra_vs_jescucha['DNI_MAGISTRADO'].apply(lambda x: x.zfill(8))
    maestra_vs_jescucha[['COD_UNIDAD_EJECUTORA', 'COD_CORTE','COD_TIPO_DEPENDENCIA','COD_DEPENDENCIA']]= maestra_vs_jescucha[['COD_UNIDAD_EJECUTORA', 'COD_CORTE','COD_TIPO_DEPENDENCIA','COD_DEPENDENCIA']].astype(float)
    maestra_vs_jescucha['NOMBRE_MAGISTRADO_y'] = maestra_vs_jescucha['NOMBRE_MAGISTRADO_y'].astype(str)
    maestra_vs_jescucha['PERFIL_USUARIO_ATENCION'] = maestra_vs_jescucha['PERFIL_USUARIO_ATENCION'].astype(str)
    maestra_vs_jescucha['Comparacion_cant'] = maestra_vs_jescucha['Comparacion_cant'].astype(str)
    maestra_vs_jescucha['Comparacion'] = maestra_vs_jescucha['Comparacion'].astype(str)
    return maestra_vs_jescucha