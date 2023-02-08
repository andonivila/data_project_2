import json
import os
import time
import random
from faker import Faker
from google.cloud import pubsub_v1
import logging
import string

###########################
### TAXI DATA GENERATOR ###
###########################

fake = Faker()

# Initial variables
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="arctic-cursor-376010-87f1bcaf048d.json" 

project_id = "arctic-cursor-376010"
topic_name = "taxi_position"

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
        logging.info("A new taxi is available. Id: %s", message['taxi_id'])

    def __exit__(self):
        self.publisher.transport.close()
        logging.info("PubSub Client closed.")

#Generate a random Spanish phone number
def generate_phone_number():
  country_code = "+34"
  primer_numero = str(6)
  segundos_3_digits = str(random.randint(1, 9999)).zfill(3)
  terceros_3_digits = str(random.randint(1, 9999)).zfill(3)
  phone_number = country_code + " " + primer_numero + segundos_3_digits + terceros_3_digits
  
  return phone_number

# Generate random 8 digit user_id
def generate_user_id():
    letters_and_digits = string.ascii_letters + string.digits
    user_id = ''.join(random.choice(letters_and_digits) for i in range(8))
    
    return user_id
    
# Generate data function
def generatedata():

    data={}
    data['taxi_id'] = generate_user_id()
    data["taxi_phone_number"] = generate_phone_number()
    data["taxi_lat"] = str(random.uniform(39.4, 39.5))
    data["taxi_lng"] = str(random.uniform(-0.4, -0.3))
    data["taxibase_fare"] = 4.00
    data["taxikm_fare"] = 1.09

    return data

# Send the data to the PUB/SUB topic
def senddata(project_id, topic_name):
    print(generatedata())
    pubsub_class = PubSubMessages(project_id, topic_name)
    
    #Publish message into the queue every 10 seconds
    try:
        while True:
            message: dict =  generatedata()
            pubsub_class.publishMessages(message)

            #it will be generated a transaction each 10 seconds
            time.sleep(10)
    except Exception as err:
        logging.error("Error while inserting data into out PubSub Topic: %s", err)
    finally:
        pubsub_class.__exit__()
    
if __name__ == "__main__":
        logging.getLogger().setLevel(logging.INFO) 
        senddata(project_id, topic_name)