import json
import os
import time
import random
import uuid

taxi_id=os.getenv('TAXI_ID')
topic_id=os.getenv('TOPIC_ID')
time_lapse=int(os.getenv('TIME_ID'))

def generatedata():
    import random
    from datetime import datetime, timedelta

    #Hora aleatoria
    def random_time():
        hour = random.randint(0, 23)
        minute = random.randint(0, 59)
        second = random.randint(0, 59)
        time = {"hour": hour, "minute": minute, "second": second}
        return time

    #Fecha aleatoria [desde hoy + tiempo ejecucion]
    def random_date():
        random_year = random.randint(1900, datetime.now().year)
        random_month = random.randint(1, 12)
        random_day = random.randint(1, 28)
        date = datetime(random_year, random_month, random_day)
        date_str = date.strftime("%Y-%m-%d")
        return date_str

    # Ubicaciones aleatorias de Valencia
    def random_location_in_Valencia():
        lat = 39.4 + random.uniform(-0.5, 0.5)
        lng = -0.4 + random.uniform(-0.5, 0.5)
        return (lat, lng)
    
    def taxi_id():
        return str(uuid.uuid4())


    data_taxi={}
    data_taxi["location"]= random_location_in_Valencia()
    data_taxi["date"]= random_date()
    data_taxi["time"]= random_time()
    data_taxi["taxi_id"]=taxi_id()

    return json.dumps(data_taxi)

def senddata():

    # Coloca el código para enviar los datos a tu sistema de mensajería
    # Utiliza la variable topic id para especificar el topico destino
    print(generatedata())



while True:
    # De los datos generados en los contenedores hay que enviarlos a pub/Sub
    senddata()
    time.sleep(time_lapse)
