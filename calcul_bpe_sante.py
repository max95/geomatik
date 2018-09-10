#!/usr/bin/env python
#-*- coding: utf-8 -*-

import psycopg2
from tqdm import tqdm
import pandas as pd
import numpy as np
import configparser
import sys

##PENSE-BETE
#http://eric.univ-lyon2.fr/~ricco/tanagra/fichiers/fr_Tanagra_Data_Manipulation_Pandas.pdf
#query_sql = "SELECT * FROM foo WHERE bar IN %s" % repr(tuple(map(str,liste_equip)))
#http://www.xavierdupre.fr/app/ensae_teaching_cs/helpsphinx/notebooks/td1a_cenonce_session_10.html


##Chargement config postgres
config = configparser.RawConfigParser()
config.read('./config')

##Lecture du fichier source
file_name_src = sys.argv[1]
#my_data = pd.read_csv('./equip-serv-sante-com-2017.csv', encoding = "utf-8", sep=';')
my_data = pd.read_csv(file_name_src, encoding = "utf-8", sep=';', dtype={'CODGEO': np.str})#dtype={'CODGEO': np.str} : force le type à string pour respecter le code insee
df = pd.DataFrame(my_data)

#Passe la colonne CODGEO en string, au cas où
#df[["CODGEO"]] = df[["CODGEO"]].astype(object)

##Creation d'un fichier echantillon
#df = df.sample(frac=0.005, replace=True)

df_final = df.copy()

#Connexion a la Base De Donnees
dsn = "host={} port={} dbname={} user={} password={}".format(config.get('POSTGRES','host'), config.get('POSTGRES','port'), config.get('POSTGRES','db'), config.get('POSTGRES','user'), config.get('POSTGRES','pass'))
conn = psycopg2.connect(dsn)
cursor = conn.cursor()

##Parcourir les colonnes à la recherche des NB_
for col in df.columns:
	col_name_src = df[col].name
	col_name_dst = "{}_TPS".format(col)
	#df_final.loc[:, col_name_dst] = 0

	if (col_name_src[0:3] == "NB_"): #Si unr colonne commene par NB_, on continu le traitement
		liste_equip = []    #Variable pour une de commune disposant de la ressource et RAZ

		#Obtenir la liste des communes disposant de la ressource
		df_equip = (df.loc[df[col_name_src]>0,:])
		
		#Creation d'une variable contenant les communes équipées de la ressource
		liste_equip = df_equip["CODGEO"].tolist()
			
		with tqdm(total=len(df)) as pbar:
			#Recherche de la commune la plus proche et récupération du tps de parcours
			for index, row in df.iterrows():
				query_sql = "SELECT \"TPS\" from matrice_depcom WHERE \"DEPCOM_START\" = '{}' AND \"DEPCOM_STOP\" IN {} order BY \"TPS\" limit 1".format(row['CODGEO'], repr(tuple(map(str,liste_equip))))
            
				try:
					cursor.execute(query_sql, liste_equip)
					TPS = cursor.fetchone()[0]
				except:
					TPS = ''
								
				#Mise à jour du tableau
				df_final.loc[df_final['CODGEO'] == row['CODGEO'] , col_name_dst] = TPS
            
				#liste_resultat.append(TPS)
				pbar.update(1)
            
		nom_fichier = "{}.csv".format(col_name_src)
		df_final.to_csv(nom_fichier, encoding='utf-8')
df_final.sort_index(axis = 1, ascending = True)
file_name_dst = '.'.join(file_name_src.split('.')[:-1])			
file_name_dst = "{}_time.csv".format(file_name_dst)
df_final.to_csv(file_name_dst, encoding='utf-8')	
