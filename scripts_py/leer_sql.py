# -*- coding: utf-8 -*-
"""
@author: Omar Paucar
"""
def leer(archivo):
    fic = open(archivo, "r")
    lines = list(fic)
    fic.close()
    lines = list(reversed(lines))
    j=''
    for i in lines:
         i=i.rstrip("\n")
         i = i + " "
         j=i+j
    return j
