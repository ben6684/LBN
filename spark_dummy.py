# coding: utf-8
#########################################################################################
# Auteur : Benoit Petitpas (bpt)
# Date : /
# Description : contruction d'une matrice flux à partir des logs
# Dernière Modification : 14/04/15
#########################################################################################
import os, sys, binstr, time

from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.mllib.clustering import KMeans
from operator import add
   
from optparse import OptionParser

################# Spark configuration ################
## TODO : Mettre ces infos dans un fichier de config
# os.environ['SPARK_HOME']="/opt/cloudera/parcels/CDH/lib/spark/"
# sys.path.append("/opt/cloudera/parcels/CDH/lib/spark/python")
# sys.path.append("/opt/cloudera/parcels/CDH/lib/spark/python/lib/py4j-0.8.2.1-src.zip")


# def get_time_field2(field):
#    return int(field.replace("-","").replace("T","").replace(":","").replace(".","").replace("Z",""))

# def parse_pli(line):
#    info = line.split(';')
#    ID = int(info[0].split(' ')[-1],16) #int
#    t = get_time_field2(info[1]) #int
#    # on va pas faire de cas général : on rentre dans le lard du truc
#    # on va en fonction 
#    if info[21]:
#       trao = info[21].split(":")[-1]
#       idrao = info[20].split(":")[-1]
#    else:
#       trao = None
#       idrao = None

#    if trao in ["defaut","rejet",""] or not trao:
#       cp ="RX"
   
#    elif trao in ["cp-commune", "ligne3", "points", "voie"]:
#       cp = "I"+idrao[:5]

#    elif trao in ["cp-menage"]:
#       cp = "P"+idrao[:5]

#    elif trao in ["cp-cedex","cp-cedex-client"]:
#       cp = info[16]

#    elif trao in ["service-interne"]:
#       cp = "SI"+idrao[3:5]

#    elif trao in ["export","villes"]:
#       cp ="EX"

#    elif trao in ["nation"]:
#       if idrao in ["FR","Fr","fr"]:
#          cp = "RX"
#       else:
#          cp = "EX"

#    else:
#       cp = "TOTO"

#    return (ID,cp,t)
   

# def reduce_centre(v1,v2):
#    return v1+v2


# def map_all(ids):
#    ID, allv = ids
#    nv = sorted(allv, key= lambda x:x[2])
#    i = 0
#    dd = nv[0][3]
#    df = nv[0][4]
#    mult = 1000000000
#    while i < len(nv):
#       j = i+1
#       if j >= len(nv): # nv est de taille 1
#          if nv[i][2] >= dd*mult and nv[i][2] < df*mult: 
#             # le mult est la pour rajouter à la date, l'heure, la minute etc...
#             yield("::".join([str(nv[i][0]),nv[i][1]]),1)
#          break
#       first = nv[i]
#       date_f  = first[2]+21000000000
#       centre_f = first[0]
#       while nv[j][2]< date_f:# test est dans les 21 jours après le premier passage
#          # test if j < i
#          j +=1
#          if j >= len(nv): # sort boucle 
#             break
#       # j vaut la premier valeur intéressante =
#       if nv[i][2] >= dd*mult and nv[i][2] < df*mult: 
#          yield("::".join([str(nv[i][0]),nv[j-1][1]]),1)
#       i = j

# def get_centres(dd,df):
#    centres =[]
#    for file in os.listdir("/depot_datas/logs_metier_prd"):
#       c = file.split(".")[1]
#       t = file.split(".")[2]
#       if int(t) in range(int(dd),int(df)):
#          if c not in centres:
#             centres.append(c)
#    return centres


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
    return [int(ID), R] 

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
    
    return [field,R , a,a_2013, a_2014,  m, j, h, mi, s, ms]

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
    return [field,R, f_PF, f_GF, p_0, p_1]
    
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
    return [field, R, e_0, e_1]

# 4: ID machine
def get_ID(field):
    R = 1
    if len(field) > 0: 
        ID = int(field)
    else:
        ID = None
        R = 0
    return [ID, R]

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
    return [field, R, t_A, t_D]

# 6: ID programme
# Garder tout
def get_IDprog(field):
    if len(field) == 0:
        R = 0
        ID = None
    else:
        ID = field
        R = 1
    return [ID, R]


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
    return [field, R, ex_in, ex_na]

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
    return [field,R, OCR_0, OCR_1]

# 10: MACAF
def get_MACAF(field):
   f = field.split(":")[-1] # on retire le premier MA : 
   ID = None
   jour = 0
   mois = 0
   an = 0
   a_2013 = 0
   a_2014 =0
   tarif = 0
   info = None    
   R = 0
   # On commence par spliter par les + :ce qui sépare tout les champs (au max 5 champs)
   if len(f) > 0:
      R = 1
      S = f.split('+')
      # On reconait chacun des éléments splité par sa taille
      # tailles: ID = 9, date = 6, tarif = 5, infos = 2 ou >10
      #for k in range(len(S)):
      #    if len(S[k]) == 9:
      ID = S[0]
      date = S[1]
      taro = S[2]
      if len(S)>3:
         info=S[3]
      if len(S)>4:
         info = info + "+" + S[4]

      ID = ID.split(" ")[-1]
      if len(date)>0:
         try:
            jour = int(date[0:2])
         except:
            jour = 0
         try:
            mois = int(date[2:4])
         except:
            mois = 0
         try:
            an = int(date[4:6])
         except:
            an =0
            print an
      a_2013 = 1
      a_2014 = 0
      if an == 2014:
         a_2014 = 1
         a_2013 = 0
      
      ta = taro.replace('H',"").replace('h',"").replace('o',"")
      if len(ta)>0:
         try:
            int(ta[-1])
         except:
            ta = ta[:-1]
         tarif = int(ta)
         
   return [f, ID, R, jour, mois, an, a_2013, a_2014, tarif, info]
    
# 11: CI
def get_CI(field):
    f = (field.split(": ")[-1]).replace("CI:","").replace("CI","")
    if len(f) == 0:
        CI = None
        R = 0
    else:
        CI = int(f,16)
        R = 1
    return [CI, R]  

# 12: Services
# tab_services = ['lettreverte', 'cedex', 'os', 'osj2', 'PND']
# On va utiliser des dummies sur cette variable

def get_services(field):
    R = 0
    services = field.split(":")[-1] # Donne la liste des services séparés par un +
    if len(services) > 0:
        R = 1
    return [services, R]    
            
# 13: Receptacle
def get_recept(field):
    R = 1
    if len(field) == 0:
        R = 0
    return [field, R]
    

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
    return [field, R, t_T, t_D]

# 15: Lot sortie
def get_lot_exit(field):
    f = field.split(':')[-1]
    if len(f) == 0:
        u = None
        R = 0
    else :
        u = int(f)
        R = 1
    return [u, R]

# 16: ID PF suivante
# On retire juste le premier élément qui est un A
def get_IDPF(field):
    R = 1
    if len(field) == 0:
        R = 0
    return [int(field[1::]), R]
   
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
              
    return [f, R, ref, jour, mois, an, a_2013, a_2014,  h, mi, s]

# 18: MTEL
 # champs trop long, on définit s'il est renseigné ou non
def get_EL(field):
    EL = 0
    f = field.split(':')[-1]
    if len(f) > 0 :
        EL = 1
    return [EL]

# 19: LEL
 # cf MTEL

# 20 et 21: RAO et type RAO
def get_RAO(f1, f2, f3):
   ID_RAO = None
   typ_RAO = None
   dep = "00"
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
   
   # dep est un string 
   liste_dep= [str(i).zfill(2) for i in range(100)]+["2A","2B"]
   l_dep = len(liste_dep)
   dummy = [0 for i in range(l_dep)]

   if dep in liste_dep:
      indice= liste_dep.index(dep)
      dummy[indice] =1
   else:
      dummy[0]=1
      print typ_RAO

   return [ID_RAO, typ_RAO,R1, R2]+dummy

##############################################################################




def dummy_line(line):
   
   infos = line.split(";")
   date = int(infos[1][8:10])
   out =[]
   if date>=18 and date<=24:
      out += get_time_field2(infos[1])[1:]
      out += get_priority(infos[2])[1:]
      out += get_state(infos[3])[1:]
      out += get_MACAF(infos[10])[2:-1]
      out += get_RAO(infos[20],infos[21],infos[16])[2:]
      yield out

if __name__ == '__main__':
   """
   USAGE : spark-submit --properties-file teralab-spark.conf spark_dummy.py 
   
   """

   ######### gestion option ############

   #####################################
   ######### configuration #############
   conf = SparkConf()
   conf.setAppName("Dummy infos")
   sc= SparkContext(conf = conf)
   #####################################

   # Get the centres
   #centres = get_centres(date_deb, date_fin)
   RDD=sc.parallelize([])

   # output path
   path = "/user/bluestone/FraudDetection/dummy_week"

   RDD= sc.textFile("/user/bluestone/logs_2014/mar/*").filter(lambda x: len(x.split(";"))>21).flatMap(dummy_line)

   RDD.cache()

   print RDD.count()

   clusters = KMeans.train(RDD, 2, maxIterations=10, runs=10, initializationMode="random")

   print RDD.map(lambda line: cluster.predict(line))

   print clusters
   print clusters.clusterCenters

   
   
   #data.coalesce(1).saveAsTextFile(path)

