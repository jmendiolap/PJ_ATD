# title LIMPIEZA DE DATOS DE LA MAESTRA
import os
import pandas as pd


def limpieza(magistrados_maestra):
    df_magistrados_maestra = pd.read_excel(magistrados_maestra)
    df_magistrados_maestra = (df_magistrados_maestra.loc[:, [
                                                                'IDUEJECOD',
                                                                'UEJECUTORA',
                                                                'COD_DISTRITO',
                                                                'DES_DISTRITO',
                                                                'COD_TIPODEPENDENCIA',
                                                                'DES_TIPODEPENDENCIA',
                                                                'COD_DEPENDENCIA',
                                                                'DES_DEPENDENCIA',
                                                                'DIRECCION',
                                                                'DNI',
                                                                'NOMBRES',
                                                                'SEXO',
                                                                'UBICA_UEJEC',
                                                                'UBICA_DISJU',
                                                                'UBICA_LOCAL',
                                                                'UBICA_DEPEND',
                                                                'CARGO'
                                                            ]
                              ])

    df_magistrados_maestra.columns = ['COD_UNIDAD_EJECUTORA',
                                      'UNIDAD_EJECUTORA',
                                      'COD_CORTE',
                                      'CORTE',
                                      'COD_TIPO_DEPENDENCIA',
                                      'TIPO_DEPENDENCIA',
                                      'COD_DEPENDENCIA',
                                      'DEPENDENCIA',
                                      'DIRECCION',
                                      'DNI_MAGISTRADO',
                                      'NOMBRE_MAGISTRADO',
                                      'SEXO_MAGISTRADO',
                                      'ubica_uejec',
                                      'ubica_disju',
                                      'ubica_local',
                                      'ubica_depend',
                                      'CARGO']
    return df_magistrados_maestra