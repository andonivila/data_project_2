from google.cloud import pubsub_v1


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
