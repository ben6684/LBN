# -*- coding: utf-8 -*-
"""
Created on Fri Apr 03 14:29:20 2015

@author: lbn
"""
import math as ma
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
from pandas import get_dummies
import time

t1 = time.time()

mon_fichier = open("C:\Users\lbn\Documents\Data\syslog.069650.20130324")
contenu = mon_fichier.read()
ligne = contenu.split("\n")
colonne = ligne[1].split(";")
a, b = len(ligne), len(colonne) 

## Création d'une liste de listes. liste de a listes chacune de taille b
liste = list(range(a))
for i in range(a):
    liste[i] = ligne[i].split(";")
  

## Passage de liste à tableau
liste = np.array(liste) 

###############################################################################

## Données à retirer:   
for i in range(a):
    # il faut retirer les données n'ayant pas 22 variables:
    if len(liste[i]) < 22:
        liste = np.delete(liste,i)
        # il faut retirer les grands formats: FAIRE A LA FIN
#    if ((liste[i])[2])[0] == 'G':
#        liste = np.delete(liste,i)
## on retire l'ancienne forme de codage pour l'adresse (RAO maintenant) donc suppression de l'élément d'indice 8
#liste = np.delete(liste,8,1)
## retirer les lettres qui passent plusieurs fois (ne conserver que leur dernier passage)

a = len(liste) # mise à jour de la taille
##############################################################################

# FONCTIONS DE TRADUCTION POUR CHACUNE DES DONNEES:

# 0: ID enveloppe

def get_ID_env(field):
    if len(field) == 0:
        R = 0
    ID = field.split(' ')[-1] # Seulement l'identifiant en base 16 à la fin
    if ID == '':
        ID = 0
    else:
        ID = int(ID,16)
        R = 1
    return int(ID), R 

# 1: Début passage
# faire plus intelligememnt

def get_time_field2(field):
    R = 1
    if len(field) == 0:
        R = 0 
    
    f1 = field.split('T')[0]
    f2 = (field.split('T')[-1]).split('.')[0]
    ms = int(((field.split('T')[-1]).split('.')[-1]).replace("Z",""))
    
    a = int(f1.split('-')[0])
    a_2013 = 1
    a_2014 = 0
    if a == 2014:
        a_2014 = 1
        a_2013 = 0
    m = int(f1.split('-')[1])
    j = int(f1.split('-')[-1])
    
    h = int(f2.split(':')[0])
    mi = int(f2.split(':')[1])
    s = int(f2.split(':')[-1])
    
    return field,R , a,a_2013, a_2014,  m, j, h, mi, s, ms

# 2: Format et priorité

def get_priority(field):
    R = 1
    f_PF = 1
    f_GF = 0
    p_0 = 0
    p_1 = 1
    if len(field) == 0:
        R = 0 
        f_PF = 0
        p_0 = 0   
    else : 
        if field[0] == 'G':
            f_PF = 0
            f_GF = 1
        if field[2] == 'N':
            p_0 = 1
            p_1 = 0             
    return field,R, f_PF, f_GF, p_0, p_1
    
# 3: Etat

def get_state(field):
    e_0 = 1
    e_1 = 0
    R = 1
    if len(field) > 0:
        if field[0] == 'C':
            e_1 = 1
            e_0 = 0
    else :
        R = 0
        e_0 = 0
    return field, R, e_0, e_1

# 4: ID machine
def get_ID(field):
    R = 1
    if len(field) > 0: 
        ID = int(field)
    else:
        ID = None
        R = 0
    return ID, R

# 5: Type programme

def get_prog(field):
    t_A = 1
    t_D = 0
    if len(field) == 0:
        t_A = 0
        R = 0
    else :
        R = 1
        if field == 'D':
            t_A = 0
            t_D = 1
    return field, R, t_A, t_D

# 6: ID programme
# Garder tout
def get_IDprog(field):
    if len(field) == 0:
        R = 0
        ID = None
    else:
        ID = field
        R = 1
    return ID, R


# 7: Exploit Machine : National ou non
# 0 = National , 1 = International
def get_exploit(field):
    if len(field) == 0:
        ex_in = 0
        ex_na = 0
        R = 0
    else:
        R = 1
        if field[0] == 'N':
            ex_in = 0
            ex_na = 1
        else : 
            ex_in = 1
            ex_na = 0
    return field, R, ex_in, ex_na

# ~8: Code adresse
# ancien code adresse, maintenant on utilise les RAO donc variable à supprimer

# 8: Origine adresse
def get_origin_lecture(field):
    OCR_0 = 0
    OCR_1 = 0
    if len(field) == 0:
        R = 0
    else:
        R = 1
        if field[0] == 'O':
            OCR_1 = 1
        else :
            OCR_0 = 1
    return field,R, OCR_0, OCR_1

# 10: MACAF
def get_MACAF(field):
    f = field.split(":")[-1] # on retire le premier MA : 
    ID = None
    jour = None
    mois = None
    an = None
    a_2013 = None
    a_2014 = None
    tarif = None
    info = None    
    R = 0
    # On commence par spliter par les + :ce qui sépare tout les champs (au max 5 champs)
    if len(f) > 0:
        R = 1
        S = f.split('+')
        # On reconait chacun des éléments splité par sa taille
        # tailles: ID = 9, date = 6, tarif = 5, infos = 2 ou >10
        for k in range(len(S)):
            if len(S[k]) == 9:
                ID = (S[k].split(" "))[-1]
            elif len(S[k]) == 6:
                jour = int((S[k])[0:2])
                mois = int((S[k])[2:4])
                an = int((S[k])[4:6])
                a_2013 = 1
                a_2014 = 0
                if an == 2014:
                    a_2014 = 1
                    a_2013 = 0
            elif len(S[k]) == 5:
                ta = (S[k]).replace('H',"").replace('h',"").replace('o',"")
                if len(ta) < 5:
                    info = S[k]
                else: 
                    tarif = int(ta)
            elif len(S[k]) == 2 or len(S[k]) > 9:
                if info == None:
                    info = S[k] 
                else: 
                    info = info + '+' + S[k]
    return f, R, ID, jour, mois, an, a_2013, a_2014, tarif, info
    
# 11: CI
def get_CI(field):
    f = (field.split(": ")[-1]).replace("CI:","").replace("CI","")
    if len(f) == 0:
        CI = None
        R = 0
    else:
        CI = int(f,16)
        R = 1
    return CI, R  

# 12: Services
# tab_services = ['lettreverte', 'cedex', 'os', 'osj2', 'PND']
# On va utiliser des dummies sur cette variable

def get_services(field):
    R = 0
    services = field.split(":")[-1] # Donne la liste des services séparés par un +
    if len(services) > 0:
        R = 1
    return services, R    
            
# 13: Receptacle
def get_recept(field):
    R = 1
    if len(field) == 0:
        R = 0
    return field, R
    

# 14: Type receptacle
def get_recept_type(field):
    if len(field) == 0:
        t_T = 0
        t_D = 0
        R = 0
    else:
        R = 1
        t_D = 1
        t_T = 0
        if field[0] == 'T':
            t_T = 1
            t_D = 0
    return field, R, t_T, t_D

# 15: Lot sortie
def get_lot_exit(field):
    f = field.split(':')[-1]
    if len(f) == 0:
        u = None
        R = 0
    else :
        u = int(f)
        R = 1
    return u, R

# 16: ID PF suivante
# On retire juste le premier élément qui est un A
def get_IDPF(field):
    R = 1
    if len(field) == 0:
        R = 0
    return int(field[1::]), R
   
# 17: Lot
def get_lot(field):
    f = field[4::] # on retire LOT:   
    ref = None
    jour = None
    mois = None
    an = None
    a_2013 = None
    a_2014 = None
    h = None
    mi = None
    s = None
    R = 0
    if len(f) > 0 :
        R = 1
        f1 = f.split(' ')
        ref = int(f1[0])
        date = f1[1]
        heure = (f1[-1])[0:8]
        
        jour = int(date.split('/')[0])
        mois = int(date.split('/')[1])
        an = int(date.split('/')[2])
        a_2014 = 0
        a_2013 = 1
        if an == 2014:
            a_2014 = 1
            a_2013 = 0
        
        h = int(heure.split(':')[0])
        mi = int(heure.split(':')[1])
        s = int(heure.split(':')[2])
              
    return f, R, ref, jour, mois, an, a_2013, a_2014,  h, mi, s

# 18: MTEL
 # champs trop long, on définit s'il est renseigné ou non
def get_EL(field):
    EL = 0
    f = field.split(':')[-1]
    if len(f) > 0 :
        EL = 1
    return EL

# 19: LEL
 # cf MTEL

# 20 et 21: RAO et type RAO
def get_RAO(f1, f2, f3):
    ID_RAO = None
    typ_RAO = None
    dep = 0
    R1 = 0
    R2 = 0
    if len(f1) > 0 :
        ID_RAO = f1.split(':')[-1]
        R1 = 1
    if len(f2) > 0 : 
        typ_RAO = f2.split(':')[-1]
        R2 = 1
    if ( len(f1) > 0 ) and ( len(f2) > 0 ) :    
        if typ_RAO in ["cp-commune", "ligne3", "points", "voie", "cp-menage"]:
            dep = ID_RAO[0:2]
     
        elif typ_RAO in ["cp-cedex","cp-cedex-client"]:
            dep = f3[1:3] 

        elif typ_RAO in ["service-interne"]:
            dep = ID_RAO[3:5]    
    return ID_RAO, R1, dep, typ_RAO, R2

##############################################################################

## LANCEMENT DE LA TRADUCTION DES DONNEES

# utilisation des foncions précédentes: conservation des variables initiales

size_liste = 81
liste_finale = range(a)
lf = range(size_liste) # liste finale avec plus de variables

for i in range(a):
    li = liste[i]    
    
    lf[0], lf[1] = get_ID_env(li[0])
    lf[2],lf[3], lf[4], lf[5], lf[6], lf[7], lf[8], lf[9], lf[10], lf[11], lf[12] = get_time_field2(li[1])
    lf[13], lf[14], lf[15], lf[16],  lf[17], lf[18] = get_priority(li[2])
    lf[19], lf[20], lf[21], lf[22] = get_state(li[3])
    lf[23], lf[24] = get_ID(li[4])
    lf[25], lf[26], lf[27], lf[28] = get_prog(li[5])
    lf[29], lf[30] = get_IDprog(li[6])
    lf[31], lf[32], lf[33], lf[34] = get_exploit(li[7])
    lf[35], lf[36], lf[37], lf[38] = get_origin_lecture(li[9])
    lf[39], lf[40], lf[41], lf[42], lf[43], lf[44], lf[45], lf[46], lf[47], lf[48], = get_MACAF(li[10]) 
    lf[49], lf[50] = get_CI(li[11])
    lf[51], lf[52] = get_services(li[12])
    lf[53], lf[54] = get_recept(li[13])
    lf[55], lf[56], lf[57], lf[58] = get_recept_type(li[14])
    lf[59], lf[60] = get_lot_exit(li[15])
    lf[61], lf[62] = get_IDPF(li[16])
    lf[63], lf[64], lf[65], lf[66], lf[67], lf[68], lf[69], lf[70], lf[71], lf[72], lf[73]  = get_lot(li[17])
    lf[74] = get_EL(li[18])
    lf[75] = get_EL(li[19])
    lf[76], lf[77], lf[78], lf[79], lf[80]= get_RAO(li[20], li[21], li[16])
    
    liste_finale[i] = np.array(lf)

t2 = time.time()
print ('Time traduc donnees', round(t2-t1))
###############################################################################

#### DUMMIFICATION DE CERTAINS CHAMPS : 

## passage en dataframe
#col = ['ID_env', 'Date Pass_Ma', 'Année', 'Année dum','mois', 'jour', 'heure', 'min', 'sec', 'ms', 'Form/prio', 'format', 'prio', 'Etat adr', 'E_A dum', 'ID_MA', 'type prog', 't_prog dum', 'ID_prog', 'Mode exploit', 'ex dum', 'lect adr', 'lec dum', 'MACAF', 'MA:ID', 'MA:j' , 'MA:m', 'MA:an','an dum', 'MA:tarif', 'MA:info', 'CI', 'CI dum', 'Services', 'Ref re', 'type re', 're dum', 'lot sortie', 'ID PS', 'LOT courier', 'LOT:ref', 'LOT:j', 'LOT:m', 'LOT:a','a dum', 'LOT:h', 'LOT:mi', 'LOT:s', 'MTEL', 'LEL', 'RAO', 'type RAO'] 
#data = pd.DataFrame(liste_finale, columns = col)
data = pd.DataFrame(liste_finale)

from pandas import get_dummies

## Dummification des services
services = data[[45]]
dum_services = pd.get_dummies( data[51])
atom_col = [c for c in dum_services.columns if ('+' not in c) and c !=""]

for col in atom_col:
    services[col] = dum_services[[c for c in dum_services.columns if col in c]].sum(axis=1) # 5 colonnes

## Dummification par départements
dep_dum = pd.get_dummies( data[78]) 

## Dummification des dates
# année : déja fait

# mois:
m1 = data[7]
mois_debpass = np.zeros((a,12), dtype = int)
for i in range(a):
    m = m1[i]-1
    (mois_debpass[i])[m] = 1
    
m2 = data[43]
mois_MA = np.zeros((a,12), dtype = int)
for i in range(a):
    m = m1[i]-1
    (mois_MA[i])[m] = 1

m3 = data[67]

mois_debpass = pd.DataFrame(mois_debpass)
mois_MA = pd.DataFrame(mois_MA)

# jour:

j1 = data[8]
jour_debpass = np.zeros((a,31), dtype = int)
for i in range(a):
    j = j1[i]-1
    (jour_debpass[i])[j] = 1
    
j2 = data[42]

j3 = data[66]
    
jour_debpass = pd.DataFrame(jour_debpass)

# jour semaine:

import calendar 
from calendar import weekday
an = 2013

joursem_debpass = np.zeros((a,7), dtype = int)

from math import isnan

for i in range(a):
    if (isnan(m1[i]) + isnan(j1[i])) == 0 :
        m = calendar.weekday(an,m1[i],j1[i])
        (joursem_debpass[i])[m] = 1  

# On enlève les lignes ou les mois et les jours ne sont pas renseignés:
m2 = m2[m2.apply(lambda x : not isnan(x))]
j2 = j2[j2.apply(lambda x : not isnan(x))]
m2.apply(int)
j2.apply(int)

m3 = m3[m3.apply(lambda x : not isnan(x))]
j3 = j3[j3.apply(lambda x : not isnan(x))]
m3.apply(int)
j3.apply(int)

jour_MA = np.zeros((len(j2),31), dtype = int)
for i in range(len(j2)):
    j = j2.iloc[i]-1
    (jour_MA[i])[j] = 1

joursem_MA = np.zeros((len(m2),7), dtype = int)
for i in range(len(m2)):
        m = calendar.weekday(an,3,int(j2.iloc[i]))
        (joursem_MA[i])[m] = 1 

joursem_LOT = np.zeros((len(m3),7), dtype = int)        
for i in range(len(m3)):
        m = calendar.weekday(an,3,int(j3.iloc[i]))
        (joursem_LOT[i])[m] = 1    

jour_LOT = np.zeros((len(j3),31), dtype = int)
for i in range(len(j3)):
    j = j3.iloc[i]-1
    (jour_LOT[i])[j] = 1      

mois_LOT = np.zeros((len(m3),12), dtype = int)
for i in range(len(m3)):
    m = m1.iloc[i]-1
    (mois_LOT[i])[m] = 1
       

joursem_debpass = pd.DataFrame(joursem_debpass)
joursem_MA = pd.DataFrame(joursem_MA)
joursem_LOT = pd.DataFrame(joursem_LOT)
jour_MA = pd.DataFrame(jour_MA)
jour_LOT = pd.DataFrame(jour_LOT)
mois_LOT = pd.DataFrame(mois_LOT)

# heure:
#note: étrange il y en a 24, a toute heure??
h_debpass = pd.get_dummies(data[9])

h = data[71]
h = h[h.apply(lambda x : not isnan(x))]
h.apply(int)
h_LOT = pd.get_dummies(h)

# Minutes:
min_debpass = pd.get_dummies(data[10])

minu = data[72]
minu = minu[minu.apply(lambda x : not isnan(x))]
minu.apply(int)
min_LOT = pd.get_dummies(minu)

## Dummification de l'info MACAF:
# 28 champs possibles
info_MA = data[[48]]
dum_MA = pd.get_dummies(data[48])
atom_col = [c for c in dum_MA.columns if ('+' not in c) and c !=""]

for col in atom_col:
    info_MA[col] = dum_MA[[c for c in dum_MA.columns if col in c]].sum(axis=1)

## Dummification de l'ID machine
# 5 diff
IF_Madum = pd.get_dummies(data[23])

## Dummification de l'ID machine suivante:
#102 diff
IF_MaSdum = pd.get_dummies(data[61])


t3 = time.time()
print ('Time Dummification', round(t3-t2))

##############################################################################

## DESCRITION DES DATA:

# Descrition data générales:
Description_generale = data.describe()

# Description services:
Description_services = services.describe()

# Date de début de passage
Description_DPmois = mois_debpass.describe()
Description_DPjour = (100000000*jour_debpass).describe() # /100000000 a faire
Description_DPjoursem = joursem_debpass.describe()
Description_DPheure = (100000000*h_debpass).describe()
Description_DPmin = (100000000*min_debpass).describe()

# MACAF
Description_MAmois = mois_MA.describe()
Description_MAjour = (100000000*jour_MA).describe()
Description_MAjoursem = joursem_MA.describe()
c = info_MA.columns
Description_MAinfo = (100000000*info_MA.loc[:,c[1::]]).describe()


# Date passage lot:
Description_LOTmois = mois_LOT.describe()
Description_LOTjour = (100000000*jour_LOT).describe()
Description_LOTjoursem = joursem_LOT.describe()
Description_LOTheure = (100000000*h_LOT).describe()
Description_LOTmin = (100000000*min_LOT).describe()

# ID machine
Description_IDMa = (100000000*IF_Madum).describe()

# ID machine suivante
Description_IDMaS = (100000000*IF_MaSdum).describe()

# Départements:
Description_dep = (100000000*dep_dum).describe()

t4 = time.time()
print ('Time Description', round(t4-t3))
###############################################################################

# EXPORTATION DES DATA FRAMES

data.to_pickle('C:\Users\lbn\Documents\\data_frames\\24.03.2013\\data24.pickle')
services.to_pickle('C:\Users\lbn\Documents\\data_frames\\24.03.2013\\services24.pickle')
mois_debpass.to_pickle('C:\Users\lbn\Documents\\data_frames\\24.03.2013\\mois_debpass24.pickle')
jour_debpass.to_pickle('C:\Users\lbn\Documents\\data_frames\\24.03.2013\\jour_debpass24.pickle')
joursem_debpass.to_pickle('C:\Users\lbn\Documents\\data_frames\\24.03.2013\\joursem_debpass24.pickle')
h_debpass.to_pickle('C:\Users\lbn\Documents\\data_frames\\24.03.2013\\h_debpass24.pickle')
min_debpass.to_pickle('C:\Users\lbn\Documents\\data_frames\\24.03.2013\\min_debpass24.pickle')
mois_MA.to_pickle('C:\Users\lbn\Documents\\data_frames\\24.03.2013\\mois_MA24.pickle')
jour_MA.to_pickle('C:\Users\lbn\Documents\\data_frames\\24.03.2013\\jour_MA24.pickle')
joursem_MA.to_pickle('C:\Users\lbn\Documents\\data_frames\\24.03.2013\\joursem_MA24.pickle')
(info_MA.loc[:,c[1::]]).to_pickle('C:\Users\lbn\Documents\\data_frames\\24.03.2013\\info_MA24.pickle')
mois_LOT.to_pickle('C:\Users\lbn\Documents\\data_frames\\24.03.2013\\mois_LOT24.pickle')
jour_LOT.to_pickle('C:\Users\lbn\Documents\\data_frames\\24.03.2013\\jour_LOT24.pickle')
joursem_LOT.to_pickle('C:\Users\lbn\Documents\\data_frames\\24.03.2013\\joursem_LOT24.pickle')
h_LOT.to_pickle('C:\Users\lbn\Documents\\data_frames\\24.03.2013\\h_LOT24.pickle')
min_LOT.to_pickle('C:\Users\lbn\Documents\\data_frames\\24.03.2013\\min_LOT24.pickle')
IF_Madum.to_pickle('C:\Users\lbn\Documents\\data_frames\\24.03.2013\\IF_Madum24.pickle')
IF_MaSdum.to_pickle('C:\Users\lbn\Documents\\data_frames\\24.03.2013\\IF_MaSdum24.pickle')
dep_dum.to_pickle('C:\Users\lbn\Documents\\data_frames\\24.03.2013\\dep_dum24.pickle')

# Descriptions sous excel

Description_generale.to_csv('C:\Users\lbn\Documents\\data_frames\\24.03.2013\\Description_generale24.csv', sep=';', na_rep='0', dtype=int)
Description_services.to_csv('C:\Users\lbn\Documents\\data_frames\\24.03.2013\\Description_services24.csv', sep=';', na_rep='0', dtype=int)
Description_DPmois.to_csv('C:\Users\lbn\Documents\\data_frames\\24.03.2013\\Description_DPmois24.csv', sep=';', na_rep='0', dtype=int)
Description_DPjour.to_csv('C:\Users\lbn\Documents\\data_frames\\24.03.2013\\Description_DPjour24.csv', sep=';', na_rep='0', dtype=int)
Description_DPjoursem.to_csv('C:\Users\lbn\Documents\\data_frames\\24.03.2013\\Description_DPjoursem24.csv', sep=';', na_rep='0', dtype=int)
Description_DPheure.to_csv('C:\Users\lbn\Documents\\data_frames\\24.03.2013\\Description_DPheure24.csv', sep=';', na_rep='0', dtype=int)
Description_DPmin.to_csv('C:\Users\lbn\Documents\\data_frames\\24.03.2013\\Description_DPmin24.csv', sep=';', na_rep='0', dtype=int)
Description_MAmois.to_csv('C:\Users\lbn\Documents\\data_frames\\24.03.2013\\Description_MAmois24.csv', sep=';', na_rep='0', dtype=int)
Description_MAjour.to_csv('C:\Users\lbn\Documents\\data_frames\\24.03.2013\\Description_MAjour24.csv', sep=';', na_rep='0', dtype=int)
Description_MAjoursem.to_csv('C:\Users\lbn\Documents\\data_frames\\24.03.2013\\Description_MAjoursem24.csv', sep=';', na_rep='0', dtype=int)
Description_MAinfo.to_csv('C:\Users\lbn\Documents\\data_frames\\24.03.2013\\Description_MAinfo24.csv', sep=';', na_rep='0', dtype=int)
Description_LOTmois.to_csv('C:\Users\lbn\Documents\\data_frames\\24.03.2013\\Description_LOTmois24.csv', sep=';', na_rep='0', dtype=int)
Description_LOTjour.to_csv('C:\Users\lbn\Documents\\data_frames\\24.03.2013\\Description_LOTjour24.csv', sep=';', na_rep='0', dtype=int)
Description_LOTjoursem.to_csv('C:\Users\lbn\Documents\\data_frames\\24.03.2013\\Description_LOTjoursem24.csv', sep=';', na_rep='0', dtype=int)
Description_LOTheure.to_csv('C:\Users\lbn\Documents\\data_frames\\24.03.2013\\Description_LOTheure24.csv', sep=';', na_rep='0', dtype=int)
Description_LOTmin.to_csv('C:\Users\lbn\Documents\\data_frames\\24.03.2013\\Description_LOTmin24.csv', sep=';', na_rep='0', dtype=int)
Description_IDMa.to_csv('C:\Users\lbn\Documents\\data_frames\\24.03.2013\\Description_IDMa24.csv', sep=';', na_rep='0', dtype=int)
Description_IDMaS .to_csv('C:\Users\lbn\Documents\\data_frames\\24.03.2013\\Description_IDMaS24.csv', sep=';', na_rep='0', dtype=int)
Description_dep.to_csv('C:\Users\lbn\Documents\\data_frames\\24.03.2013\\Description_dep24.csv', sep=';', na_rep='0', dtype=int)

t5 = time.time()
print ('Time Exportation', round(t5-t4))


# On le lit directement comme un dataframe:
# a = read_pickle('C:\Users\lbn\Documents\\data_frames\\19.03.2013\\data19.pickle')

###############################################################################

tend = time.time() 
print('Time total', round(tend - t1))

