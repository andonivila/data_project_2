import json
import time
import uuid
import random
import logging
import argparse
from faker import Faker 
from datetime import datetime
from google.cloud import pubsub_v1

fake = Faker()

parser = argparse.ArgumentParser(description=('Aixigo Contracts Dataflow pipeline.'))

parser.add_argument(
                '--project_id',
                required=True,
                help='GCP cloud project name.')
parser.add_argument(
                '--topic_name',
                required=True,
                help='PubSub topic name.')
                
args, opts = parser.parse_known_args()

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
        logging.info("A New transaction has been registered. Id: %s", message['Transaction_Id'])

    def __exit__(self):
        self.publisher.transport.close()
        logging.info("PubSub Client closed.")



def generador_datos ():
    product_id = random.choice(['pF8z9GBG', 'XsEOhUOT', '89x5FhyA', 'S3yG1alL', '5pz386iG'])
    client_name = fake.name()
    transaction_id = str(uuid.uuid4())
    transaction_tmp = str(datetime.now())
    transaction_amount = fake.random_number(digits=5)
    frequent_client = random.choice([True, False])
    payment_method = "credit_card"
    credit_card_number = fake.credit_card_number() if payment_method == "credit_card" else None 
    email = fake.email() if frequent_client == True else None

      #Return values
    return {
        "Name": client_name,
        "Transaction_Id": transaction_id,
        "Transaction_Amount": transaction_amount,
        "Credit_Card_Number": credit_card_number,
        "Transacion_Timestamp": transaction_tmp,
        "Is_Registered": frequent_client,
        "Email": email,
        "Product_Id": product_id
    }


def run_generator(project_id, topic_name):
    pubsub_class = PubSubMessages(project_id, topic_name)
    #Publish message into the queue every 5 seconds
    try:
        while True:
            message: dict = generador_datos()
            pubsub_class.publishMessages(message)
            #it will be generated a transaction each 2 seconds
            time.sleep(5)
    except Exception as err:
        logging.error("Error while inserting data into out PubSub Topic: %s", err)
    finally:
        pubsub_class.__exit__()

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run_generator(args.project_id, args.topic_name)