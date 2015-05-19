# -*- coding: utf-8 -*-
"""
Created on Fri May 15 09:57:09 2015

@author: lbn
"""

# traduction des données de dataframe à matrice sparse
import pandas as pd
from pandas import read_pickle
from pandas import concat
from pandas import get_dummies
import math as ma
import numpy as np
import scipy.sparse as sp
from scipy import sparse, io
from sys import getsizeof
from scipy.sparse import csr_matrix
import time

t0 = time.time()
#Fam = 'Fam1'  
#col = [2,3]
#Nom = 'services'


#df_in_3 = ['data', 'IF_Madum']
#col = [21,27,36,37,57]
# 
def df_to_sparse (jour,df_in, Fam, col): 
    
    # INPUTS:
    # num = jour considéré
    # df_in = nom du dataframe d'entrée à traiter
    # Fam = famille considéré 
    # col = colonne à conserver du data frame d'entrée vers celui de sortie (si
    # col = 0 on prend tout le dataframe)
    
    # OUTPUT:
    # Il n'y a pas d'output, le fichier est enregistré sous format de sparse
    # matrix
    
    # FONCTION:
    dossier = str(jour) +'.03.2013'
    path_entree = 'C:\Users\lbn\Documents\\data_frames\\'
    path_sortie = 'C:\Users\lbn\Documents\\data_frames\\sparse_data\\'
    doc_entree = path_entree + dossier +'\\'+df_in + str(jour) +'.pickle'
    doc_sortie = path_sortie + dossier +'\\'+df_in+ str(jour)+Fam
    
    data = read_pickle(doc_entree)
    if type(col) == list :
        data = csr_matrix(data.iloc[:,col], dtype=np.int8) 
    else :
        data = csr_matrix(data, dtype=np.int8) 
    io.mmwrite(doc_sortie, data)

    print('Traduction effectuee de:', df_in+ str(jour)+Fam)


for jour in range(18,25):
    Fam = '_Fam4'
    df_in = 'IF_MaSdum'    
    df_to_sparse(jour, df_in, Fam, 0)
        
        
t1 = time.time()
print('Traduction Fam4', round(t1-t0))


###############################################################################
##                  RECUPERER LES DEPARTEMENTS:
##############################################################################

def get_dep_sparse (jour): 
    
    df_in = 'data' + str(jour) +'Fam4_dep'
    Fam = '_Fam4'
    
    # FONCTION:
    dossier = str(jour) +'.03.2013'
    #path_entree = 'C:\Users\lbn\Documents\\data_frames\\'
    path_sortie = 'C:\Users\lbn\Documents\\data_frames\\sparse_data\\'
    doc_entree = path_sortie + dossier +'\\'+df_in + '.mtx'
    doc_sortie = path_sortie + dossier +'\\'+'dep_dum'+ str(jour)+Fam
    
    data = io.mmread(doc_entree)
    df = pd.DataFrame(data.toarray())    
    dep_dum = pd.get_dummies(df[0])
    dep_dum = csr_matrix(dep_dum, dtype=np.int8) 
    io.mmwrite(doc_sortie, dep_dum)
    print('Traduction effectuee de:', 'dep_dum'+ str(jour)+Fam)
    
###############################################################################
    
for jour in range(18,25):
    get_dep_sparse(jour)
    