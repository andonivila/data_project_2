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

# Generación de una posición random en la ciudad de Valencia
def generate_random_position():
    x = 0
    y = 0
    coordinates = [(x, y)]
    for i in range(10):
        direction = random.choice(["N", "S", "E", "W"])
    if direction == "N":
        y += 1
    elif direction == "S":
        y -= 1
    elif direction == "E":
        x += 1
    else:
        x-= 1
    coordinates.append((x, y))
    return coordinates

trip = simulate_taxi_trip()
for i, (x, y) in enumerate(trip):
print(f"Step {i}: ({x}, {y})")
    latitude = random.uniform(39.4, 39.5)
    longitude = random.uniform(-0.4, -0.3)
    return (latitude, longitude)

def generatedata():

    data={}
# Generate taxi data:

    data["userid"]=user_id

    # Generate user data
# User ID: a unique identifier for each user.
# Name: the user's first and last name.
# Phone number: the user's phone number.
# Email: the user's email address.
# Address: the user's home address.
# Payment information: the user's payment method, such as a credit card or PayPal.
# User type: whether the user is a rider or a driver.
# Rating: the user's rating as a rider or driver.
        
    data["userid"]=user_id
    data["user_name"]=fake.name()
    data["phone_number"]="+34 " + "6".join([str(random.randint(1,9)) for i in range(9)])
    data["email"]=fake.email()
    data["location"]=generate_random_position()
    data["payment_method"]=random.choice(['Credir card', 'Paypal'])
    data["user_type"] = random.choice(['driver', 'rider'])
    data["timestamp"] = datetime.datetime.now().isoformat()

    return json.dumps(data)

def senddata():

    # Coloca el código para enviar los datos a tu sistema de mensajería
    # Utiliza la variable topic id para especificar el topico destino
    print(generatedata())





while True:
    senddata()
    time.sleep(time_lapse)
