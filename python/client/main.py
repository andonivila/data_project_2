import json
import os
import time
import random

user_id=os.getenv('USER_ID')
topic_id=os.getenv('TOPIC_ID')
time_lapse=int(os.getenv('TIME_ID'))

def generatedata(type):
    import random

    def random_time():
        hour = random.randint(0, 23)
        minute = random.randint(0, 59)
        second = random.randint(0, 59)
        return "{:02d}:{:02d}:{:02d}".format(hour, minute, second)

        print(random_time())

    random_time()
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
