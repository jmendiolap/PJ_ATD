# -*- coding: utf-8 -*-
"""
Created on Mon Oct 11 09:45:35 2021

@author: Aryan
"""
def leer_sql(archivo):
    fic = open(archivo, "r")
    lines = list(fic)
    fic.close()
    lines = list(reversed(lines))
    j=''
    for i in lines:
         i=i.rstrip("\n")
         j=i+j
    return j
