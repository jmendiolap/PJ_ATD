import pandas as pd
def resumenes(maestra_vs_sesiones,maestra_vs_resoluciones,maestra_vs_notificaciones,maestra_vs_fallos):
#def resumenes(maestra_vs_sesiones, maestra_vs_resoluciones, maestra_vs_notificaciones, maestra_vs_fallos):
    m_sesiones = (maestra_vs_sesiones.loc[:, [
                                                 'CORTE_x',
                                                 'Comparacion',
                                                 'FECHA_global',
                                             ]
                  ])
    m_sesiones = m_sesiones.assign(ORIGEN="SESIONES")
    # ----------------------------------------------------------------------------------------------------
    # m_marcaciones=(maestra_vs_marcaciones.loc[:,[
    #                                                        'CORTE_x',
    #                                                        'Comparacion',
    #                                                        'FECHA_global',
    #                                                    ]
    #                                                       ])
    # m_marcaciones=m_marcaciones.assign(ORIGEN="MARCACIONES")
    # ----------------------------------------------------------------------------------------------------
    m_resoluciones = (maestra_vs_resoluciones.loc[:, [
                                                         'CORTE_x',
                                                         'Comparacion',
                                                         'FECHA_global',
                                                     ]
                      ])
    m_resoluciones = m_resoluciones.assign(ORIGEN="RESOLUCIONES")
    # ----------------------------------------------------------------------------------------------------
    m_notificaciones = (maestra_vs_notificaciones.loc[:, [
                                                             'CORTE_x',
                                                             'Comparacion',
                                                             'FECHA_global',
                                                         ]
                        ])
    m_notificaciones = m_notificaciones.assign(ORIGEN="NOTIFICACIONES")
    # ----------------------------------------------------------------------------------------------------
    m_fallos = (maestra_vs_fallos.loc[:, [
                                             'CORTE_x',
                                             'Comparacion',
                                             'FECHA_global',
                                         ]
                ])
    m_fallos = m_fallos.assign(ORIGEN="FALLOS")
    # ----------------------------------------------------------------------------------------------------


    # ----------------------------------------------------------------------------------------------------
    df_resumen = [m_sesiones, m_resoluciones, m_notificaciones, m_fallos]
    #df_resumen = [m_sesiones, m_resoluciones, m_notificaciones, m_fallos]
    df_resumen = pd.concat(df_resumen, ignore_index=True, sort=False)
    cant = 1 / len(df_resumen.index)
    df_resumen = df_resumen.assign(contador_resumen=1)

    # ----------------------------------------------------------------------------------------------------
    FECHA_g = df_resumen['FECHA_global'][0]
    resumen_x = df_resumen.groupby(['CORTE_x', 'Comparacion']).count()
    # lista_criterio1 = list(resumen_x['Comparacion'])
    resumen_x.reset_index(inplace=True)
    resumen_x = (resumen_x.loc[:, ['CORTE_x',
                                   'Comparacion',
                                   'contador_resumen']
                 ])

    l_corte = df_resumen.groupby(['CORTE_x']).count()
    l_corte.reset_index(inplace=True)
    l_corte = (l_corte.loc[:, ['CORTE_x',
                               'contador_resumen']
               ])
    l_corte.reset_index(inplace=True)
    resumen_x.reset_index(inplace=True)
    resumen_x = resumen_x.merge(
        l_corte,
        how='outer',
        indicator='Comparacion_s',
        left_on=
        ['CORTE_x'
         ]
        , right_on=
        ['CORTE_x'
         ]
    )
    resumen_x = (resumen_x.loc[:, ['CORTE_x',
                                   'Comparacion',
                                   'contador_resumen_x',
                                   'contador_resumen_y',
                                   ]
                 ])

    resumen_x['percent'] = resumen_x['contador_resumen_x'] / resumen_x['contador_resumen_y']
    resumen_x.columns = ['CORTE_x',
                         'ESTADO',
                         'CANTIDAD_MAGISTRADO',
                         'CANTIDAD_X_CORTE',
                         'PORCENTAJE'
                         ]

    resumen_x = resumen_x.assign(FECHA_global=FECHA_g)
    return df_resumen, resumen_x
