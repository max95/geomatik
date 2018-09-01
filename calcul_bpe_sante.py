import psycopg2
from tqdm import tqdm
import pandas as pd
import numpy as np
import configparser

##PENSE-BETE
#http://eric.univ-lyon2.fr/~ricco/tanagra/fichiers/fr_Tanagra_Data_Manipulation_Pandas.pdf
#query_sql = "SELECT * FROM foo WHERE bar IN %s" % repr(tuple(map(str,liste_equip)))
#http://www.xavierdupre.fr/app/ensae_teaching_cs/helpsphinx/notebooks/td1a_cenonce_session_10.html


##Chargement config postgres
config = configparser.RawConfigParser() 
config.read('./config')

##Lecture du fichier source
my_data = pd.read_csv('./equip-serv-sante-com-2017.csv', encoding = "utf-8", sep=';')
df = pd.DataFrame(my_data)

##Creation d'un fichier echantillon
df_sample = df.sample(frac=0.005, replace=True)
df_final = df_sample['CODGEO'].copy

#Connexion a la Base De Donnees
dsn = "host={} port={} dbname={} user={} password={}".format(config.get('POSTGRES','host'), config.get('POSTGRES','port'), config.get('POSTGRES','db'), config.get('POSTGRES','user'), config.get('POSTGRES','pass'))
conn = psycopg2.connect(dsn)
cursor = conn.cursor()

##Parcourir les colonnes à la recher des NB_
for col in df.columns:
    col_name = df[col].name
    
    if (col_name[0:3] == "NB_"): #Si unr colonne commene par NB_, on continu le traitement
            liste_equip = []    #Variable pour une de commune disposant de la ressource et RAZ
            liste_resultat = [] #Pour préparer le tableau final et RAZ
            
            #Obtenir la liste des communes disposant de la ressource
            df_equip = (df_sample.loc[df[col_name]>0,:])
            
#            print(col_name)
            
            #Creation d'une variable contenant les communes équipées de la ressource
            liste_equip = df_equip["CODGEO"].tolist()

                    
            #####DEBUG
#            print(liste_equip)
            
            with tqdm(total=len(df_sample)) as pbar:
                    #Recherche de la commune la plus proche et récupération du tps de parcours
                    for index, row in df_sample.iterrows():
                        ##DEBUG
                        #print(row['CODGEO'])
                        
#                        query_sql = "SELECT \"TPS\" from matrice_depcom WHERE \"DEPCOM_START\" = '" + row['CODGEO'] + "' AND \"DEPCOM_STOP\" IN %s order BY \"TPS\" limit 1" % repr(tuple(map(str,liste_equip)))
                        query_sql = "SELECT \"TPS\" from matrice_depcom WHERE \"DEPCOM_START\" = '{}' AND \"DEPCOM_STOP\" IN {} order BY \"TPS\" limit 1".format(row['CODGEO'], repr(tuple(map(str,liste_equip))))

                            
                        try:
                                cursor.execute(query_sql, liste_equip)
                                TPS = cursor.fetchone()[0]

                        except:
                                #print("erreur SQL")
                                TPS = 'NULL'

                        liste_resultat.append(TPS)
                        pbar.update(1)

                    ##Ajout d'une colonne audataframe final avec les resultat précédent
                    df_final.loc[:, col_name] = liste_resultat
                    print(df_final)


