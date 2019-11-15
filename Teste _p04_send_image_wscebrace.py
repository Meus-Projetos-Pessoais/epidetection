# -*- coding: utf-8 -*-
"""
Created on Wed Jun  5 13:37:54 2019

@author: aw1001
"""
import time
from datetime import datetime
import cv2
import base64
import pandas as pd
import json
import requests
import datetime
import numpy as np
import datetime
import re
import dask.dataframe as dd
import sqlite3 as sqlite
import time
import asyncio
import time
import bd_connect


import mysql.connector
mydb = mysql.connector.connect(
  host="localhost",
  user="aw1001",
  passwd="Cbc5678*",
  database="safety_camera"
)


#conexão com o webservice
#url_cebrace_salvar = "http://sistemas.cebrace.com.br/EzTask.WebAPI/api/ocorrencia/salvar"
#url_cebrace_update = "http://sistemas.cebrace.com.br/EzTask.WebAPI/api/ocorrencia/id/adicionar-foto"
url_cebrace_connect = "http://sistemas.cebrace.com.br/EzTask.WebAPI/api/token"


#conn = sqlite.connect('safety_camera1.db')
#cursor = conn.cursor()
#conn = bd_connect.mydb.cursor()
#df = pd.read_sql_query("SELECT * FROM images Where status = 10", conn, index_col =None)
print("Iniciando ...")

cursor =  mydb.cursor(buffered=True,dictionary=True)
query=   ("SELECT * FROM images Where status = 10")
cursor.execute(query)
df =  pd.read_sql(query,mydb,index_col="idimage")

print("Consulta ao banco efetuada ...")

def connect_webservice_cebrace(username, password):
    #url_cebrace_connect = "http://sistemas.cebrace.com.br/EzTask.WebAPI/api/token"
    headers_cebrace_connect = { "content-type": "application/x-www-form-urlencoded" }
    data_cebrace_connect = { "grant_type": "password", "username": username, "password": password }
    response_conect = requests.post(url_cebrace_connect, data = data_cebrace_connect, headers = headers_cebrace_connect)
    response_conect_json = response_conect.json()

    return response_conect_json["access_token"], response_conect_json["expires_in"]

access_token, expires_in = connect_webservice_cebrace("KEYRUSAPI", "SW2019")
headers_cebrace = { "content-type": "application/json", "Authorization": ("Bearer " + access_token) }
#headers_cebrace

def envioImagen(atividade, anexo):
    #print(atividade)
    url_cebrace_update = "http://localhost:22700/api/ocorrencia/novaAtividadeID/adicionar-foto"
    idocorrencia = ''
    #valorEnvioFoto += 1
    dadosEnvioImagemRestate =  json.dumps({"atividadeID": atividade,"anexo" : jpg_as_text})
    #idocorrencia =  str(ocorrenciaSalvaEzTask['_novaAtividadeID'])
    #print(idocorrencia)
    atividade =  str(atividade)

    #if imagemRestante.empty:
        #break
    #else:
    #jpg_as_text = imagemRestante['processed_image ']
    #jpg_as_text = jpg_as_text.encode("UTF-8")
    #jpg_as_text = base64.b64encode(jpg_as_text).decode('utf-8')
    #print(jpg_as_text)
    url_cebrace_update = url_cebrace_update.replace('novaAtividadeID',atividade)
    print(idocorrencia)
    print(url_cebrace_update)
    dadosEnvioImagemRestate =  json.dumps({
                                          "anexo" : jpg_as_text,
                                          #"anomalia": anomaly_txt
                                          })
    #print(dadosEnvioImagemRestate)
    response_cebrace_update = requests.put(url_cebrace_update,data = dadosEnvioImagemRestate,headers = headers_cebrace)

    respostaDadosEnvioImagemRestate =  json.loads(response_cebrace_update.text)
    print("Resposta do sistema  :",respostaDadosEnvioImagemRestate['mensagem'])

print("Iniciando o agrupamento ... ")
    
now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
#print('acquisition_time XXXXXXX', df['acquisition_time'].iloc[:1][0])
df['acquisition_time'] = pd.to_datetime(df.acquisition_time)
df['acquisition_time'] = df['acquisition_time'].dt.floor('120s')
df['acquisition_time_group'] = df['acquisition_time'].factorize()[0]
#agrupamentosCamera = df.groupby('acquisition_time_group', axis= 0 )
agrupamentosCamera = df.groupby(['acquisition_time_group'], axis= 0 )

'''
Factorize, retorna o valor distinto dentro do conjunto de dados.
'''
print("Iniciando o envio...")
# itera em cada grupo
for indiceAgrupamento, agrupamentoCamera in agrupamentosCamera:  
    #print(agrupamentoCamera.iterrows())
    valorRequisicao += 1
    primeiroRegistroCamera = agrupamentoCamera
    #print(agrupamentoCamera.iterrows())
    #print(primeiroRegistroCamera)
    idcamera = str(primeiroRegistroCamera['idcamera'].values[0])
    #print((idcamera))
    #acquisition_time = str(primeiroRegistroCamera['acquisition_time'])
    
    acquisition_time_formatted = pd.to_datetime(str(primeiroRegistroCamera['acquisition_time'].values[0])).strftime('%Y-%m-%d %H:%M:%S')
    
    update_time = primeiroRegistroCamera['update_time']
    status = primeiroRegistroCamera['status']
    json_returned = primeiroRegistroCamera['json_returned']
    anomalies = json_returned
    #print(anomalies)
    anomaly_txt= ""
    
    for item in anomalies:
        if item == "":
            anomaly_txt += 'Anomalia não detectada'
        if  re.findall(r'\b(\w*safety\w*)\b', item ) ==['safety']:
            anomaly_txt += " \nAndando fora da faixa de seguranca "
        elif re.findall(r'\b(\w*no helmet\w*)\b', item ) == ['no helmet']:
            anomaly_txt += " \nFalta de capacete "
        elif re.findall(r'\b(\w*no vest\w*)\b', item ) == ['no vest']:
            anomaly_txt += " \nFalta de colete "
    
    eztask_return = primeiroRegistroCamera['eztask_return']
    raw_image = primeiroRegistroCamera['raw_image']
    processed_image = primeiroRegistroCamera['processed_image']
    anomaly_group = primeiroRegistroCamera['anomaly_group']
    accuracy_check = primeiroRegistroCamera['accuracy_check']
    
    jpg_as_text = str(primeiroRegistroCamera['processed_image'])
    jpg_as_text = jpg_as_text.encode("UTF-8")
    jpg_as_text = base64.b64encode(jpg_as_text).decode('utf-8')
    
    data_cebrace =  {
        "dataInicial": acquisition_time_formatted,
        "dataFinal":now, 
        "camera": ("Unidade JCR - Camera: " + str(idcamera) ),
        "anomalia": anomaly_txt,
        "anexo": jpg_as_text
    }
    
    
    data_cebrace = json.dumps(data_cebrace)
    #print(data_cebrace)
    response_cebrace_salvar = requests.post(url_cebrace_salvar, data = data_cebrace, headers = headers_cebrace)
    print(response_cebrace_salvar.text)
    
    if response_cebrace_salvar.status_code != 200:
        
        break        
            
    else:
        ocorrenciaSalvaEzTask = json.loads(response_cebrace_salvar.text)
        #print(ocorrenciaSalvaEzTask['id'])
        
        imagensRestantesCamera = agrupamentoCamera[1:]
        #print(imagensRestantesCamera)
        
        
        for indice , imagemRestante in imagensRestantesCamera.iterrows():
            asyncio.run(envioImagen(ocorrenciaSalvaEzTask['_novaAtividadeID'], jpg_as_text))
            
            

    

