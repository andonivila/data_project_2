import json
import os
import time
import random
from faker import Faker
import datetime
from google.cloud import pubsub_v1
import logging


fake = Faker()

user_id=os.getenv('USER_ID')
topic_id=os.getenv('TOPIC_ID')
time_lapse=int(os.getenv('TIME_ID'))
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="data-project-2-376316-a19138ce1e45.json" 

project_id = "data-project-2-376316"
topic_name = "user_position"

## PUB/SUB class declaration
class PubSubMessages:
    """ Publish Messages in our PubSub Topic """

    def __init__(self, project_id, topic_name):
        self.publisher = pubsub_v1.PublisherClient()
        self.project_id = project_id
        self.topic_name = topic_name

    def publishMessages(self, message):
        json_str = json.dumps(message)
        topic_path = self.publisher.topic_path(self.project_id, self.topic_name)
        self.publisher.publish(topic_path, json_str.encode("utf-8"))
        logging.info("A new user is looking for a ride. Id: %s", message['userid'])

    def __exit__(self):
        self.publisher.transport.close()
        logging.info("PubSub Client closed.")

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


    return data


def senddata(project_id, topic_name):
    print(generatedata())
    pubsub_class = PubSubMessages(project_id, topic_name)
    #Publish message into the queue every 5 seconds
    try:
        while True:
            message: dict =  generatedata()
            pubsub_class.publishMessages(message)
            #it will be generated a transaction each 2 seconds
            time.sleep(time_lapse)
    except Exception as err:
        logging.error("Error while inserting data into out PubSub Topic: %s", err)
    finally:
        pubsub_class.__exit__()
    


if __name__ == "__main__":
        logging.getLogger().setLevel(logging.INFO)
        senddata(project_id, topic_name)