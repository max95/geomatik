#!/usr/bin/env python
#-*- coding: utf-8 -*-

import psycopg2 
import requests 
import json 
from threading import Thread, RLock
import time
import configparser

##Chargement config postgres
config = configparser.RawConfigParser() 
config.read('./config')

verrou = RLock()
total_import_sql = ''

dsn = "host={} port={} dbname={} user={} password={}".format(config.get('POSTGRES','host'), config.get('POSTGRES','port'), config.get('POSTGRES','db'), config.get('POSTGRES','user'), config.get('POSTGRES','pass'))

class Geo(Thread):

	"""Thread chargé de calculer le temps de parcours."""
	

	def __init__(self, start, finish, start_long, start_lat, finish_long, finish_lat):
		Thread.__init__(self)
		self.depcom_start = start
		self.depcom_finish = finish
		self.start_long = str(start_long)
		self.start_lat = str(start_lat)
		self.finish_long = str(finish_long)
		self.finish_lat = str(finish_lat)
		
	def run(self):
		"""Code à executer pendant l'exécution du Thread."""
		global total_import_sql
		#global update_sql
		
		##DEBUG - Test retour de variable en entrée
		#print(self.depcom_start + " - " + self.depcom_finish) 
		#print("http://51.254.121.49:5000/table/v1/driving/" + self.start_long + "," + self.start_lat + ";" + self.finish_long + "," + self.finish_lat)

		try:
			r = requests.get("http://51.254.121.49:5000/table/v1/driving/" + self.start_long + "," + self.start_lat + ";" + self.finish_long + "," + self.finish_lat)
			data = r.text
			data = json.loads(data)
			if str(data['code']) == "Ok":
				##DEBUG - test le restour serveur OSRM
				#print(self.depcom_start + " - " + self.depcom_finish + " : " + str(data['durations'][1][0]))
				with verrou:
					total_import_sql += ('UPDATE matrice_depcom SET "TPS" = ' + str(data['durations'][1][0]) + ' WHERE "DEPCOM_START" = \'' + self.depcom_start + '\' AND "DEPCOM_STOP" = \'' +self.depcom_finish + '\' AND "TPS" is null;')
				#update_sql.execute('UPDATE matrice_depcom SET "TPS" = ' + str(data['durations'][1][0]) + ' WHERE "DEPCOM_START" = \'' + self.depcom_start + '\' AND "DEPCOM_STOP" = \'' +self.depcom_finish + '\' AND "TPS" is null;')	
		except:
			#print(".")
			pass
		#print('UPDATE matrice_depcom SET "TPS" = ' + str(data['durations'][1][0]) + ' WHERE "DEPCOM_START" = \'' + self.depcom_start + '\' AND "DEPCOM_STOP" = \'' +self.depcom_finish + '\' AND "TPS" is null;')
		#update_sql.execute('UPDATE matrice_depcom SET "TPS" = ' + str(data['durations'][1][0]) + ' WHERE "DEPCOM_START" = \'' + self.depcom_start + '\' AND "DEPCOM_STOP" = \'' +self.depcom_finish + '\' AND "TPS" is null;')
def action():
	global cursor
	
	
	query_sql = 'SELECT "DEPCOM_START", "DEPCOM_STOP", a."LONG" as "START_LONG", a."LAT" as "START_LAT", b."LONG" as "STOP_LONG", b."LAT" as "STOP_LAT" FROM matrice_depcom, commune_2018 as a, commune_2018 as b WHERE "DEPCOM_START" = a."DEPCOM" AND "DEPCOM_STOP" = b."DEPCOM" AND "TPS" is null Limit 20000' 

	try:
		cursor.execute(query_sql)
		print(cursor.fetchone()[0])
		 
	except:
		#action()
		print("Erreur_requete_commune")

	for row in cursor:
		thread = Geo(row[0], row[1], row[2], row[3], row[4], row[5])
		try:
			thread.start()
		except:
			pass

	#time.sleep(5)


	
	
conn = psycopg2.connect(dsn)
#conn.autocommit = True
cursor = conn.cursor()
update_sql = conn.cursor()
print("test_")
	
while True:
	

	time.sleep(1)
	total_import_sql = ''
	try: 
		cursor.execute(query_sql)
		total = cursor.rowcount
		print("Total : " + str(total))	
	except:
		#print("erreur")
		total = 1
	if total <= 0:
            print("*************************STOP******************************")
            break
	else:
		action()	
		#print("Commit")
		update_sql.execute(total_import_sql)
		conn.commit()
		
#print(total_import_sql)
cursor.close()
conn.close()
