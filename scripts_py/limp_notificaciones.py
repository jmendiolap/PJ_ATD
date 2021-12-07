# @title LIMPIEZA Y TRATAMIENDO DE DATOS DE notificaciones
import os
import pandas as pd
import import_ipynb
import scripts_py.unir_mismo as unir_mismo

def limpieza(archivo_notificaciones_ase, archivo_notificaciones_asa, df_magistrados_maestra,fecha2):
    ##df_notificaciones_ase= unir_mismo.unir(archivo_notificaciones_ase)
    df_notificaciones_ase = pd.read_excel(archivo_notificaciones_ase)
    df_notificaciones_ase = (df_notificaciones_ase.loc[:, [
                                                              'fecha',
                                                              'corte',
                                                              'desper',
                                                              'c_usuario',
                                                              'x_nom_usuario',
                                                              'c_dni',
                                                              'cantidad_notificaciones'
                                                          ]
                             ])
    df_notificaciones_asa = pd.read_excel(archivo_notificaciones_asa)
    df_notificaciones_asa = (df_notificaciones_asa.loc[:, [
                                                              'fecha',
                                                              'corte',
                                                              'desper',
                                                              'c_usuario',
                                                              'x_nom_usuario',
                                                              'c_dni',
                                                              'cantidad_notificaciones'
                                                          ]
                             ])
    df_notificaciones = [df_notificaciones_ase, df_notificaciones_asa]
    df_notificaciones = pd.concat(df_notificaciones, ignore_index=True, sort=False)
    # se quita los duplicados
    df_notificaciones = df_notificaciones.drop_duplicates(subset=['c_dni'])

    df_notificaciones.columns = [
        'FECHA',
        'CORTE',
        'PERFIL',
        'C_USUARIO',
        'NOMBRE_MAGISTRADO',
        'DNI_MAGISTRADO',
        'CANT_NOTIFICACIONES'
    ]
    # COMPARACION SESIONES VS MAGISTRADOS
    maestra_vs_notificaciones = df_magistrados_maestra.merge(
        df_notificaciones,
        how='outer',
        indicator='Comparacion',
        left_on=
        ['DNI_MAGISTRADO'
         ]
        , right_on=
        ['DNI_MAGISTRADO'
         ]
    )
    maestra_vs_notificaciones['FECHA'] = pd.to_datetime(maestra_vs_notificaciones['FECHA'])
    maestra_vs_notificaciones['FECHA'] = [d.strftime('%Y-%m-%d') if not pd.isnull(d) else '' for d in
                                          maestra_vs_notificaciones['FECHA']]
    maestra_vs_notificaciones['FECHA'] = pd.to_datetime(maestra_vs_notificaciones['FECHA'], format='%Y/%m/%d')
    a = maestra_vs_notificaciones['FECHA'].unique().tolist()
    a = pd.to_datetime(a, format='%Y-%m-%d')
    a = a.strftime('%Y/%d/%m')
    maestra_vs_notificaciones = maestra_vs_notificaciones.assign(FECHA_global=fecha2)
    n_registros_maestra_vs_notificaciones = maestra_vs_notificaciones.shape[0]
    maestra_vs_notificaciones['N_REGISTROS'] = n_registros_maestra_vs_notificaciones
    maestra_vs_notificaciones['DNI_MAGISTRADO'] = maestra_vs_notificaciones['DNI_MAGISTRADO'].astype(str)
    maestra_vs_notificaciones['DNI_MAGISTRADO'] = maestra_vs_notificaciones['DNI_MAGISTRADO'].apply(
        lambda x: x.zfill(8))

    return maestra_vs_notificaciones