# -*- coding: utf-8 -*-
"""
Created on Mon May 11 14:41:29 2015

@author: lbn
"""
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
from sklearn.decomposition import PCA
import matplotlib.pyplot as plt
from sklearn import cluster



###############################################################################
###############################################################################
##                  FONCTIONS CREATION FAMILLES
###############################################################################
###############################################################################
      
def creation_famille(Fam, names_concat):
    path_entree = 'C:\Users\lbn\Documents\\data_frames\\sparse_data\\'
    path_sortie = 'C:\Users\lbn\Documents\\data_frames\\sparse_data\\Familles\\'
    #l = len(names_concat)
    famille = 0

    
    for jour in range(18,25):
        famille_jour = 0
        
        for name in names_concat:
            entree = path_entree + str(jour) +'.03.2013'+ '\\'+ name + str(jour) + Fam
            sparse_matrix = io.mmread(entree)
            if type(famille_jour) == int:
                famille_jour = sparse_matrix
            else:
                famille_jour = sp.hstack((famille_jour, sparse_matrix), format = 'csr') 
                
        print ('jour',jour)
        
        famille_jour = pd.DataFrame(famille_jour.toarray())
        if type(famille) == int :
            famille = famille_jour
        else:
            #famille = sp.vstack((famille, famille_jour), format = 'csr')  
            famille = concat([famille, famille_jour], axis = 0)

    famille = csr_matrix(famille, dtype=np.int8)     
    #sortie = path_sortie + Fam
    #io.mmwrite(sortie, famille)
    print('Famille generee', Fam)            
    
    return famille
 

###############################################################################
###############################################################################
##                       FAMILLES
###############################################################################
###############################################################################
 
###############################################################################
##                  FAMILLE 1: Colis
###############################################################################
    
#concaténation de:
    
    # data[15,17,33,34,47,50,52,74]
    # R_MACAF.loc[:,[2,3]]
    # info_MA
    # services
    
# Fam1 = (concat([data.loc[:,[15,17,33,34,47,50,52,74]],R_MACAF.loc[:,[2,3]], info_MA, services], axis = 1)).fillna(value=0)
# Fam1.columns = ['']

Fam = '_Fam1'
names_concat = ['data', 'R_MACAF', 'info_MA', 'services']

t0 = time.time()
famille1 = creation_famille(Fam, names_concat)
t1 = time.time()
print('time famille 1', round(t1-t0))

###############################################################################
##                  FAMILLE 2: Date passage
###############################################################################

## Fam2_m:    
#concaténation de:
    
    # mois_debpass
    # mois_LOT
    # mois_MA
    
#Fam2_m = (concat([mois_debpass, mois_LOT, mois_MA], axis = 1)).fillna(value=0)

Fam = '_Fam2_m'
names_concat = ['mois_debpass', 'mois_LOT', 'mois_MA']

t0 = time.time()
famille2_m = creation_famille(Fam, names_concat)
t1 = time.time()
print('time famille 2_m', round(t1-t0))
    
###############################################################################

## Fam2_j:    
#concaténation de:
    
    # jour_debpass
    # jour_LOT
    # joursem_debpass
    # joursem_LOT
    # jour_MA
    # joursem_MA
    
#Fam2_j = (concat([jour_debpass, jour_LOT, joursem_debpass, joursem_LOT, jour_MA, joursem_MA], axis = 1)).fillna(value=0)

Fam = '_Fam2_j'
names_concat = ['joursem_debpass', 'joursem_LOT', 'joursem_MA']
#[ 'jour_debpass', 'jour_LOT', 'joursem_debpass', 'joursem_LOT', 
                #'jour_MA', 'joursem_MA' ]
t0 = time.time()
famille2_j = creation_famille(Fam, names_concat)
t1 = time.time()
print('time famille 2_j', round(t1-t0))


    
###############################################################################

## Fam2_h:    
#concaténation de:
    
    # h_debpass
    # h_LOT
    
# Fam2_h = (concat([h_debpass, h_LOT], axis = 1)).fillna(value=0)
    
Fam = '_Fam2_h'
names_concat = [ 'h_debpass', 'h_LOT']

t0 = time.time()
famille2_h = creation_famille(Fam, names_concat)
t1 = time.time()
print('time famille 2_h', round(t1-t0))

    
###############################################################################
##                  FAMILLE 3: Paramètres techniques
###############################################################################
    
#concaténation de:
    
    # data.loc[:,[21,27,36,37,57]
    # IF_Madum
    
#Fam3 = (concat([data.loc[:,[21,27,36,37,57]],IF_Madum], axis = 1)).fillna(value=0)
    
Fam = '_Fam3'
names_concat = [ 'data', 'IF_Madum' ]

t0 = time.time()
famille3 = creation_famille(Fam, names_concat)
t1 = time.time()
print('time famille 3', round(t1-t0))

    
###############################################################################
##                  FAMILLE 4: Géographie
###############################################################################
    
#concaténation de:
    
    # IF_MaSdum
    # dep_dum

#Fam4 = (concat([IF_MaSdum, dep_dum], axis = 1)).fillna(value=0)
#Fam4_pca =  pd.DataFrame(pca4.fit_transform(Fam4))
   
Fam = '_Fam4'
names_concat = [ 'IF_MaSdum', 'dep_dum' ]

t0 = time.time()
famille4 = creation_famille(Fam, names_concat)
t1 = time.time()
print('time famille 4', round(t1-t0))
# à renommer dans le dossier sinon la 2e Fam4 va écraser la première

Fam = '_Fam4'
names_concat = [ 'dep_dum' ]

t0 = time.time()
famille4 = creation_famille(Fam, names_concat)
t1 = time.time()
print('time famille 4', round(t1-t0))
    
###############################################################################
##                  FAMILLE 5: Variables renseignées R_
###############################################################################
    
#concaténation de:
    
    # data.iloc[:,[40,50,52,64]]

# Fam5 = data.iloc[:,[40,50,52,64]]
#Fam5.columns = ['R_MACAF', 'R_CI', 'R_Serv','R_LOT']

Fam = '_Fam5'
names_concat = [ 'data' ]

t0 = time.time()
famille5 = creation_famille(Fam, names_concat)
t1 = time.time()
print('time famille 5', round(t1-t0))

###############################################################################

###############################################################################
###############################################################################
##                       PCA
###############################################################################
###############################################################################

famille4_1 = io.mmread('C:\Users\lbn\Documents\\data_frames\\sparse_data\\Familles\\_Fam4_1.mtx')
famille4_2 = io.mmread('C:\Users\lbn\Documents\\data_frames\\sparse_data\\Familles\\_Fam4_2.mtx')

# ref_fam = '2_j'
#n = nb de réduction de var dans pca
def pca_fam (famille, ref_fam, n):
    
    t0 = time.time()    
    
    pca4 = PCA(n_components=n, whiten = True)
    pca = pca4.fit(famille.toarray())
    poids = pca.components_
    poids = pd.DataFrame(poids)
    
    out = 'C:\Users\lbn\Documents\\data_frames\\sparse_data\\' +'poids_fam' + ref_fam + '.csv'
    poids.to_csv(out, sep=';', na_rep='0', dtype=int)
    
    VA_red = csr_matrix( pca4.fit_transform(famille.toarray()) , dtype = np.int8)
    sortie = 'C:\Users\lbn\Documents\\data_frames\\sparse_data\\kmeans\\' + 'VA_red' + ref_fam
    io.mmwrite(sortie, VA_red)    
    
    explain_value = pca.explained_variance_ratio_
    print('explain value de', ref_fam, ':::', explain_value) 
    
    t1 = time.time()
    print('time pca', famille, round(t1 - t0))
    
    return pca, VA_red

## Ne pas charger les familles complétes sur 7 jours mais les regénérer

t1 = time.time()
pca1, VA_red1 = pca_fam(famille1, '1', 4)
t2 = time.time()
print('time pca fam1', round(t2-t1)) 

t1 = time.time()    
pca2_j, VA_red2_j = pca_fam(famille2_j, '2_j', 4)  
t2 = time.time()    
print('time pca fam2_j', round(t2-t1)) 

t1 = time.time()    
pca2_m, VA_red2_m = pca_fam(famille2_m, '2_m', 4)  
t2 = time.time()
print('time pca fam2_m', round(t2-t1)) 

t1 = time.time()    
pca2_h, VA_red2_h = pca_fam(famille2_h, '2_h', 4)  
t2 = time.time()
print('time pca fam2_h', round(t2-t1)) 

t1 = time.time()    
pca3, VA_red3 = pca_fam(famille3, '3', 4)  
t2 = time.time()
print('time pca fam3', round(t2-t1)) 

t0 = time.time()
pca4_1, VA_red4_1 = pca_fam(famille4_1, '4_1', 4)
t1 = time.time()
print('time pca fam5', round(t1-t0)) 

t0 = time.time()
pca4_2, VA_red4_2 = pca_fam(famille4_2, '4_2', 4)  
t1 = time.time()
print('time pca fam5', round(t1-t0)) 

t0 = time.time()
pca4, VA_red4 = pca_fam([VA_red4_1, VA_red4_2], '4', 4) 
t1 = time.time()
print('time pca fam5', round(t1-t0)) 

t1 = time.time()    
pca5, VA_red5 = pca_fam(famille5, '5', 2)   
t2 = time.time()
print('time pca fam5', round(t2-t1)) 

VA_red = concat([VA_red1, VA_red2_j, VA_red2_m, VA_red2_h, VA_red3, VA_red4,
                 VA_red5])


###############################################################################
###############################################################################
##                       K-MEANS
###############################################################################
###############################################################################



k_means = cluster.KMeans(n_clusters=2, n_init=4)
k_means.fit(VA_red)
values = k_means.cluster_centers_.squeeze()
labels = k_means.labels_

card_c1 = sum(labels == 0)
card_c2 = sum(labels == 1)

ratio1 = card_c2/float(card_c1)
ratio2 = card_c1/float(card_c2)
print('cardinal cluster 1 :', card_c1, 'cardinal cluster 2 :', card_c2)
print('ratio', min(ratio1,ratio2))

labels = csr_matrix(labels, dtype=np.int8) #pd.DataFrame(labels)
io.mmwrite('C:\Users\lbn\Documents\\data_frames\\sparse_data\\kmeans\\labels_VAR.mtx',labels)
#labels.to_pickle('C:\Users\lbn\Documents\\data_frames\\kmeans\\labels_VAR.pickle')


# Observation des clusters : 
#cluster1 = f[(labels[0]==0)]
#cluster2 = f[(labels[0]==1)]