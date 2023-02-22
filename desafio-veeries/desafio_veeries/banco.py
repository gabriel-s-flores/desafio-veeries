import json
import os
import sqlite3
import time
from pyowm import OWM
from pyowm.utils import config
from pyowm.utils import timestamps

owm = OWM('d129dc0b18b2bcf83a9778a926c6d14e')
mgr = owm.weather_manager()


def preencheBanco(banco, cursor):
    
    with open('municipios.json', encoding = 'utf-8-sig') as f:
            dadosMunicipios = json.load(f)
            municipiosCentroOeste = list(filter(lambda m: m['codigo_uf'] in range(50,60), dadosMunicipios))
    

            for i in range(len(municipiosCentroOeste)):    
                climaNoMunicipio = mgr.weather_at_coords(municipiosCentroOeste[i]['latitude'], municipiosCentroOeste[i]['longitude'])
                w = climaNoMunicipio.weather
        
                if municipiosCentroOeste[i]['codigo_uf'] == 50:
                    uf = ("MS")
                elif municipiosCentroOeste[i]['codigo_uf'] == 51:
                    uf = ("MT")
                elif municipiosCentroOeste[i]['codigo_uf'] == 52:
                    uf = ("GO")
                elif municipiosCentroOeste[i]['codigo_uf'] == 53:
                    uf = ("DF")
            
                if '1h' in w.rain:
                    chuva = w.rain['1h']
                else:
                    chuva = 0.0
            
        
                municipioExistente = cursor.execute("SELECT * FROM centro_oeste WHERE codigo = ?", (municipiosCentroOeste[i]['codigo_ibge'],)).fetchone()

                if municipioExistente:
                    cursor.execute("UPDATE centro_oeste SET umidade = ?, temperatura = ?, chuvas = ? WHERE codigo = ?", (str(w.humidity), str(w.temperature('celsius')['temp']), str(chuva), municipiosCentroOeste[i]['codigo_ibge']))
                    print("atualizou o banco de dados o municipio de "+municipiosCentroOeste[i]['nome']+"-"+uf+"")
                else:    
                    cursor.execute("INSERT OR REPLACE INTO centro_oeste VALUES(?,?,?,?,?,?)", (municipiosCentroOeste[i]['nome'],uf,str(municipiosCentroOeste[i]['codigo_ibge']),str(w.humidity),str(w.temperature('celsius')['temp']),str(chuva)))
                    print("inseriu no banco de dados o municipio de "+municipiosCentroOeste[i]['nome']+"-"+uf+"")
        
                banco.commit()
                

def inicializaBanco():
      
    if os.path.isfile('clima.db'):
        modified_time = os.path.getmtime('clima.db')
        current_time = time.time()
        if (current_time - modified_time) < 24 * 60 * 60:
            print('O banco de dados foi atualizado nas últimas 24h')
            return
        else:
            print('O banco de dados não foi atualizado nas últimas 24h')
            banco = sqlite3.connect('clima.db')
            cursor = banco.cursor()
            cursor.execute("CREATE TABLE IF NOT EXISTS centro_oeste (nome text, uf text, codigo integer, umidade integer, temperatura float, chuvas float)")
            preencheBanco(banco, cursor)
            return
    else:
        print('O banco não existe e será criado')
        banco = sqlite3.connect('clima.db')
        cursor = banco.cursor()
        cursor.execute("CREATE TABLE IF NOT EXISTS centro_oeste (nome text, uf text, codigo integer, umidade integer, temperatura float, chuvas float)")
        preencheBanco(banco, cursor)
        return


        



