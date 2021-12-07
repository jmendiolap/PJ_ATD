# @title LIMPIEZA Y TRATAMIENDO DE DATOS DE SESIONES - CONECCION AL SIJ
# SESIONES:
import pandas as pd
from openpyxl import load_workbook
import scripts_py.unir_mismo as unir_mismo

def limpieza(archivo_sesiones_asa, archivo_sesiones_ase, df_magistrados_maestra,fecha2):
    df_sesiones_ase = unir_mismo.unir(archivo_sesiones_ase)
    df_sesiones_ase = (df_sesiones_ase.loc[:, [ 'corte',
                                                'fecha',
                                                'x_nom_instancia',
                                                'desper',
                                                'c_usuario',
                                                'x_nom_usuario',
                                                'c_dni',
                                                'cant_sesion_x_dia',
                                                ]
                       ])
    df_sesiones_asa = pd.read_excel(archivo_sesiones_asa)
    df_sesiones_asa = (df_sesiones_asa.loc[:, [ 'corte',
                                                'fecha',
                                                'x_nom_instancia',
                                                'desper',
                                                'c_usuario',
                                                'x_nom_usuario',
                                                'c_dni',
                                                'cant_sesion_x_dia',
                                                ]
                       ])
    df_sesiones = [df_sesiones_ase, df_sesiones_asa]
    df_sesiones = pd.concat(df_sesiones, ignore_index=True, sort=False)
    # SUMA SI DNIs
    lista_criterio1 = list(df_sesiones["c_dni"])
    sumar_si_conjunto = [df_sesiones.loc[(df_sesiones['c_dni'] == lista_criterio1[i]), "cant_sesion_x_dia"].sum() for i
                         in range(df_sesiones.shape[0])]
    df_sesiones["cant_total_sesion_x_dia"] = sumar_si_conjunto

    # se quita los duplicados
    df_sesiones = df_sesiones.drop_duplicates(subset=['c_dni'])

    df_sesiones.columns = ['CORTE',
                           'FECHA',
                           'INSTANCIA',
                           'PERFIL',
                           'USUARIO_JUEZ',
                           'NOMBRE_MAGISTRADO',
                           'DNI_MAGISTRADO',
                           'CANTIDAD_SESION_X_DIA',
                           'CANTIDAD_TOTAL_SESION_X_DIA'
                           ]
    # COMPARACION SESIONES VS MAGISTRADOS
    maestra_vs_sesiones = df_magistrados_maestra.merge(
        df_sesiones,
        how='outer',
        indicator='Comparacion',
        left_on=
        ['DNI_MAGISTRADO'
         ]
        , right_on=
        ['DNI_MAGISTRADO'
         ]
    )

    maestra_vs_sesiones['FECHA'] = pd.to_datetime(maestra_vs_sesiones['FECHA'])
    maestra_vs_sesiones['FECHA'] = [d.strftime('%Y-%m-%d') if not pd.isnull(d) else '' for d in
                                    maestra_vs_sesiones['FECHA']]
    maestra_vs_sesiones['FECHA'] = pd.to_datetime(maestra_vs_sesiones['FECHA'], format='%Y/%m/%d')
    a = maestra_vs_sesiones['FECHA'].unique().tolist()
    a = pd.to_datetime(a, format='%Y-%m-%d')
    a = a.strftime('%Y/%d/%m')
    maestra_vs_sesiones = maestra_vs_sesiones.assign(FECHA_global=fecha2)
    n_registros_maestra_vs_sesiones = maestra_vs_sesiones.shape[0]
    maestra_vs_sesiones['N_REGISTROS'] = n_registros_maestra_vs_sesiones
    maestra_vs_sesiones['DNI_MAGISTRADO'] = maestra_vs_sesiones['DNI_MAGISTRADO'].astype(str)
    maestra_vs_sesiones['DNI_MAGISTRADO'] = maestra_vs_sesiones['DNI_MAGISTRADO'].apply(lambda x: x.zfill(8))
    maestra_vs_sesiones
    return maestra_vs_sesiones