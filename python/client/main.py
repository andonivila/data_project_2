import json
import os
import time
import random

user_id=os.getenv('USER_ID')
topic_id=os.getenv('TOPIC_ID')
time_lapse=int(os.getenv('TIME_ID'))

def generatedata(type):
    import random
    from datetime import datetime, timedelta

    #Hora aleatoria
    def random_time():
        hour = random.randint(0, 23)
        minute = random.randint(0, 59)
        second = random.randint(0, 59)
        return "{:02d}:{:02d}:{:02d}".format(hour, minute, second)

        print(random_time())

    #Fecha aleatoria [desde hoy + tiempo ejecucion]
    def random_date():
        random_year = random.randint(1900, datetime.now().year)
        random_month = random.randint(1, 12)
        random_day = random.randint(1, 28)
        return datetime(random_year, random_month, random_day)

    # Ubicaciones aleatorias de Valencia
    import random

    def random_location_in_Valencia():
        lat = 39.4 + random.uniform(-0.5, 0.5)
        lng = -0.4 + random.uniform(-0.5, 0.5)
        return (lat, lng)

    random_location_in_Valencia()
    random_date()
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
