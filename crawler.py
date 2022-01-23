#!/usr/local/bin/python3


#importing libraries

#import os
import csv

from termcolor import colored

import datetime
import json
import urllib.request

import pandas as pd 
from pandas.io.json import json_normalize #package for flattening json in pandas df

import time

from cassandra.cluster import Cluster
import warnings
from cassandra.auth import PlainTextAuthProvider
from cassandra.cqlengine import connection


####################################

def url_builder(city_id,city_name,country):
    user_api = 'f2573f2c32c60b5915dbcaba22fbbbb8'  # chaged to personnal key, Obtain yours form: http://openweathermap.org/
    unit = 'metric'  # For Fahrenheit use imperial, for Celsius use metric, and the default is Kelvin.
    if(city_name!=""):
        api = 'http://api.openweathermap.org/data/2.5/weather?q=' # "http://api.openweathermap.org/data/2.5/weather?q=Tunis,fr
        full_api_url = api + str(city_name) +','+ str(country)+ '&mode=json&units=' + unit + '&APPID=' + user_api
    else:
        api = 'http://api.openweathermap.org/data/2.5/weather?id='     # Search for your city ID here: http://bulk.openweathermap.org/sample/city.list.json.gz
        full_api_url = api + str(city_id) + '&mode=json&units=' + unit + '&APPID=' + user_api
   
    return full_api_url


#################################################

def data_fetch(full_api_url):
    url = urllib.request.urlopen(full_api_url)
    output = url.read().decode('utf-8')
    raw_api_dict = json.loads(output)
    url.close()
    return raw_api_dict

##################################################################

def time_converter(time):
    converted_time = datetime.datetime.fromtimestamp(
        int(time)
    ).strftime('%I:%M %p')
    return converted_time

###################################################################

def data_organizer(raw_api_dict):
    data = dict(
        id_station=raw_api_dict.get('id'),
        city=raw_api_dict.get('name'),
        country=raw_api_dict.get('sys').get('country'),
        temp=raw_api_dict.get('main').get('temp'),
        temp_max=raw_api_dict.get('main').get('temp_max'),
        temp_min=raw_api_dict.get('main').get('temp_min'),
    )
    print (data)
    return data

#######################################################################

def data_output(data):
    m_symbol = '\xb0' + 'C'
    print('---------------------------------------')
    print('Current weather in: {}, {}:'.format(data['city'], data['country']))
    print(data['temp'], m_symbol)
    print('Max: {}, Min: {}'.format(data['temp_max'], data['temp_min']))
    print('---------------------------------------')

###########################################################################

def WriteCSV(data):
    with open('mael_weatherOpenMap.csv', 'a') as f:  # Just use 'w' mode in 3.x
        w = csv.DictWriter(f, data.keys())
        w.writeheader()
        w.writerow(data)


##################################################################################

def getVilles():
    with open('city.list.json') as f:
        d = json.load(f)
        villes=pd.DataFrame(d)
        return villes

villes = getVilles()
villes_france = villes[villes["country"]=='FR']['id']
#############################################################################

################################################"
def pandas_factory(colnames, rows):
    return pd.DataFrame(rows, columns=colnames)
#on force ici a repecter le datframe de pandas lors de la recuperation des données

if __name__ == '__main__':
    try:
        city_name=''
        country='FR'
        request_number = 0
        
        
        
        for ville in villes_france.iloc[0:50]:
            if request_number > 58:
                print('Quota de requêtes/minute en approche, sleep 1min1s')
                time.sleep(61)
                request_number = 0
            
            city_id = ville
            #Generation de l url
            print(colored('Generation de l url ', 'red',attrs=['bold']))
            url=url_builder(city_id,city_name,country)
            
            #Invocation du API afin de recuperer les données
            print(colored('Invocation du API afin de recuperer les données', 'red',attrs=['bold']))
            data=data_fetch(url)
            
            #Formatage des données
            print(colored('Formatage des donnée', 'red',attrs=['bold']))
            data_orgnized=data_organizer(data)
            
            #Affichage de données
            print(colored('Affichage de données ', 'red',attrs=['bold']))
            data_output(data_orgnized)

            
            #Enregistrement des données à dans un fichier CSV 
            print(colored('Enregistrement des données à dans un fichier CSV ', 'green',attrs=['bold']))
            WriteCSV(data_orgnized)
            
            #Lecture des données a partir de fichier CSV 
            #data=ReadCSV()
            #print(colored('Affichage des données lues de CSV ', 'green',attrs=['bold']))
            
            
            #Affichage des données 
            #data_output(data)
            request_number += 1

        
    except IOError:
        print('no internet')

    # compile: connect, crée le keyspace et la table
    france_df = pd.read_csv('mael_weatherOpenMap.csv')
    france_df = france_df[france_df['temp_max']!='temp_max']
    france_df['city'] = france_df['city'].str.replace("'",' ')
    france_df['id_station'] = france_df['id_station'].astype(int)
    france_df['temp'] = france_df['temp'].astype(float)
    france_df['temp_min'] = france_df['temp_min'].astype(float)
    france_df['temp_max'] = france_df['temp_max'].astype(float)
    #france_df.to_csv('clean_weather_France.csv')
    
    CASSANDRA_HOST = ['172.21.0.3']
    CASSANDRA_PORT = 9042
    auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra')
    
    try:
        cluster = Cluster(contact_points=CASSANDRA_HOST, 
                          port=CASSANDRA_PORT, 
                          auth_provider = auth_provider)
        session = cluster.connect()
        session.row_factory = pandas_factory
    
    except ValueError:
        print("Oops!  échec de connexion cluster.  Try again...")
    
    session.execute("CREATE KEYSPACE IF NOT EXISTS weather_fr WITH REPLICATION={'class':'SimpleStrategy','replication_factor':3};")
    
    session.execute('USE weather_fr;')
    
    session.execute('CREATE TABLE IF NOT EXISTS weather_fr.temperatures ( id_station INT, country TEXT, city TEXT, temp FLOAT, temp_min FLOAT, temp_max FLOAT, primary key (id_station));')
    
    rows=session.execute('Select * from weather_fr.temperatures;')
    df_results = rows._current_rows
    df_results.head()
    
    session = cluster.connect('weather_fr')  
    session.row_factory = pandas_factory
    session.default_fetch_size = 10000000 #needed for large queries, otherwise driver will do pagination. Default is 50000.
    
    query_insert="INSERT INTO weather_fr.temperatures (id_station, country, city, temp, temp_min, temp_max) \
VALUES (%s, %s, %s, %s, %s, %s);"
    
    
    for ct in france_df.index:
        session.execute(query_insert,(france_df['id_station'][ct],
                                        france_df['country'][ct],
                                        france_df['city'][ct],
                                        france_df['temp'][ct],
                                        france_df['temp_min'][ct],
                                        france_df['temp_max'][ct]))
 
print('Scrapping finished.')

while True:

    print('start : %s' % time.ctime())
    time.sleep(15000)

print('End: %s' % time.ctime())