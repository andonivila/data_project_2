import json
import os
import time
import random
from faker import Faker
import datetime


fake = Faker()

user_id=os.getenv('USER_ID')
topic_id=os.getenv('TOPIC_ID')
time_lapse=int(os.getenv('TIME_ID'))

## Clase de pubsub

# Generación de una posición random en la ciudad de Valencia
def generate_random_position():
    latitude = random.uniform(39.4, 39.5)
    longitude = random.uniform(-0.4, -0.3)
    return (latitude, longitude)

def generate_phone_number():
  country_code = "+34"
  primer_numero = str(6)

  segundos_3_digits = str(random.randint(1, 9999)).zfill(3)
  terceros_3_digits = str(random.randint(1, 9999)).zfill(3)

  
  phone_number = country_code + " " + primer_numero + segundos_3_digits + terceros_3_digits
  return phone_number

# Generating random data
name = fake.name()
phone_number = generate_phone_number()
email = fake.email()
location = generate_random_position()
payment_method = random.choice(['Credit card', 'Paypal', 'Cash'])

def generatedata():

    data={}
    data["userid"]=user_id
    data["user_name"]= name
    data["phone_number"]=phone_number
    data["email"]=email
    data["location"]=location
    data["payment_method"]=payment_method
    data["timestamp"] = str(datetime.datetime.now())


    return json.dumps(data)

def senddata():

    # Coloca el código para enviar los datos a tu sistema de mensajería
    # Utiliza la variable topic id para especificar el topico destino
    print(generatedata())


while True:
    senddata()


    time.sleep(time_lapse)
