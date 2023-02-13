# GENERADOR DE DATOS TAXI

## 1. útiles para el código
### 1.1. Librerias
```python
import json
import os
import time
import random
from faker import Faker
from google.cloud import pubsub_v1
import logging
import string

#Se importan las librerias necearias para poder ejecutar el codigo correctamente
```
### 1.2. Variables 
```python
fake = Faker()

# Initial variables
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="data-project-2-376316-a19138ce1e45.json" 

project_id = "data-project-2-376316"
topic_name = "taxi_position"

# Se declaran las variables inciales. Por un lado "os.environ", donde se representa un diccionario que contiene las variables de entorno del sistema operativo en formato JSON. "project_id", es el nombre del proyecto en google cloud donde se va a a desarrollar la estructura de dataflow a posteriori, y finalmente el topic_name, será donde estarán destinados los mensajes a enviarse para guardarse en Pub/Sub
```
## 2.Funciones y clases

### 2.1. Clase para mandar mensajes a PubSub
```python
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
        logging.info("A new taxi is available. Zone_id: %s", message['zone_id'])

    def __exit__(self):
        self.publisher.transport.close()
        logging.info("PubSub Client closed.")

#Se crea una clase denominada "PubSubMessages", la cual contiene tres funciones: una de inicialización, otra de ejecución de la clase y finalmente una de finalización.

#En la de ejecución, introducciendo dos parámetros como el message:
def publishMessages(self, message):
#(que será dato del generador creado) y el self que es la función de antes para poder incializarla.

#Posteriormente mediante 
json_str = json.dumps(message)
#se utiliza para convertir un objeto Python en una cadena JSON. 

#Con 
topic_path = self.publisher.topic_path(self.project_id, self.topic_name)
#se determinada la ruta donde estára el tópico dentro de un id_project creado al cual hacer referencia. 

#Finalmente con:
self.publisher.publish(topic_path, json_str.encode("utf-8"))logging.info("A new taxi is available. Zone_id: %s", message['zone_id'])

#"El primer argumento, topic_path, es la ruta del tópico en Google Cloud Pub/Sub al que se quiere publicar el mensaje. El segundo argumento, json_str.encode("utf-8"), es el contenido del mensaje codificado como una secuencia de bytes en formato UTF-8.
```

### 2.2. Funcion generar un telefono aleatorio español

```python
def generate_phone_number():
  country_code = "+34"
  primer_numero = str(6)
  segundos_3_digits = str(random.randint(1, 9999)).zfill(3)
  terceros_3_digits = str(random.randint(1, 9999)).zfill(3)
  phone_number = country_code + " " + primer_numero + segundos_3_digits + terceros_3_digits
  
  return phone_number
```
### 2.3. Generar usuarios i con 8 dígitos aleatorios

```python
def generate_user_id():
    letters_and_digits = string.ascii_letters + string.digits
    user_id = ''.join(random.choice(letters_and_digits) for i in range(8))
    
    return user_id
```

### 2.4. Generador de datos Taxi
```python
data={
        "zone_id" : random.randint(1,5),
        "payload": {
            "user_id": generate_user_id(),
            "taxi_phone_number" : generate_phone_number(),
            "taxi_lat" : str(random.uniform(39.4, 39.5)),
            "taxi_lng" : str(random.uniform(-0.4, -0.3)),
            "taxibase_fare" : str(random.uniform(39.4, 39.5)), 
            "taxikm_fare" : str(random.uniform(-0.4, -0.3)),
            "zone_id" : random.randint(1,5),
            }
        }

    return data

#Finalmente por cada conductor de taxi, se obtendrán las variables: zone_id, ubicación del taxi delimitado por taxi_lat y taxi_lng, precio del taxi mediante taxibse_fare y taxikm_fare
```

## 2.5. Enviar el mensaje al tópico de PUB/SUB
```python
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

# Ahora se procederá a explicar la principales claves de esta función
```

```python
def senddata(project_id, topic_name):
#Cuando se declare la función, es necesario pasarle dos variables, que serán el nombre del project de google cloud, y el nombre del tópico al que se van a lanzar los mensajes
```
```python
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
# Mediante un bucle infintito se generan los datos aleatorios mediante la función "generatedata()" cada 10 segundos mediante "time.sleep(10)". 

# Mediante la funcion "pubsub_class.publishMessages(message)" llama a la clase nombrada al principio para poder ejecutar la acción de mandar el mensaje.

# En caso de que no haya sido posible mandar el mensaje a pubsub se emitirá el siguiente error "Error while inserting data into out PubSub Topic: %s", err"
```

```python
if __name__ == "__main__":
        logging.getLogger().setLevel(logging.INFO) 
        senddata(project_id, topic_name)

#Mediante está funcion se ejecuta la función sendata creada justo antes, la cual coge las dos variables declaradas al principio del script, "project_id" y "topic_name"
```