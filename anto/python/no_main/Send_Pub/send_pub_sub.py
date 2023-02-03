from google.cloud import pubsub_v1
import argparse
import logging
import json


def send_mensagge (project_id, topic_name):
    #Escribir el ID del proyecto
    project_id = "ID->DEL_PROYECTO_AQUI"

    #Nombre del topic donde queramos publicar
    topic_name = "TOPIC->DEL_PROYECTO_AQUI"

    # Preparacion del cliente de Pub/Sub 
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_name)

    # Publica el mensaje y esperar a que se complete el proceso
    message = " " <-- EL MENSAJE AQUI 

    future = publisher.publish(topic_path, data=message.encode("utf-8"))
    result = future.result()

    return print("Message published with ID: {}".format(result))

--------------------------------------------------------------------------------------
parser = argparse.ArgumentParser(description=('Aixigo Contracts Dataflow pipeline.'))
parser.add_argument(
                '--project_id',
                required=True,
                help='GCP cloud project name.')
parser.add_argument(
                '--topic_name',
                required=True,
                help='PubSub topic name.')

class PubSubMessages:
    """ Publish Messages in our PubSub Topic """

    def __init__(self, project_id, topic_name):
        self.publisher = pubsub_v1.PublisherClient()
        self.project_id = project_id
        self.topic_name = topic_name

    def publishMessages(self, message):
        json_str = json.dumps(message)
        topic_path = self.publisher.topic_path(self.project_id, self.topic_name)
        publish_future = self.publisher.publish(topic_path, json_str.encode("utf-8"))
        logging.info("A New transaction has been registered. Id: %s", message['Transaction_Id'])

    def __exit__(self):
        self.publisher.transport.close()
        logging.info("PubSub Client closed.")


