import json
import os
import time
import random

user_id=os.getenv('USER_ID')
topic_id=os.getenv('TOPIC_ID')
time_lapse=int(os.getenv('TIME_ID'))

def generatedata(type):

    # Generate taxi data 
    if type == "taxi":
        data={}
        data["userid"]=user_id

    # Generate user data
    elif type == "user":
        data={}
        data["userid"]=user_id

    return json.dumps(data)

def senddata():

    # Coloca el código para enviar los datos a tu sistema de mensajería
    # Utiliza la variable topic id para especificar el topico destino
    print(generatedata())



while True:
    senddata()
    time.sleep(time_lapse)
