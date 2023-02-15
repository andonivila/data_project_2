import json
import os
import time
import random
from faker import Faker
from google.cloud import pubsub_v1
import logging
import string

###########################
### USER DATA GENERATOR ###
###########################

fake = Faker()

# Initial variables
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
        logging.info("A new user is looking for a taxi. User_Id: %s", message['user_id'])

    def __exit__(self):
        self.publisher.transport.close()
        logging.info("PubSub Client closed.")


# Generate random 8 digit user_id
def generate_user_id():
    letters_and_digits = string.ascii_letters + string.digits
    user_id = ''.join(random.choice(letters_and_digits) for i in range(8))
    
    return user_id

# Generate data function
def generatedata():

    data={}
    data["user_id"] = generate_user_id()
    data["userinit_lat"] = str(random.uniform(39.4, 39.5))
    data["userinit_lng"] = str(random.uniform(-0.4, -0.3))
    data["userfinal_lat"] = str(random.uniform(39.4, 39.5))
    data["userfinal_lng"] = str(random.uniform(-0.4, -0.3))

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