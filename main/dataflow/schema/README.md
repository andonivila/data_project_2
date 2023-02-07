# ESTRUCTURA TAXI DATAFLOW
## Importar librerias BEAM
En primer lugar se importarán las librearías neceasrias para poder ejecutar en el entorno BEAM

Pero antes, se procederá a definir rápidamente que es Apache Beam: "Apache Beam es un modelo de programación unificado de código abierto para definir y ejecutar canalizaciones de procesamiento de datos, incluido ETL, procesamiento por lotes y secuencias."
![LOGO](https://miro.medium.com/max/1280/0*vjTWLBDhlm_14_8C.png)

```python
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.combiners import MeanCombineFn
from apache_beam.transforms.combiners import CountCombineFn
from apache_beam.transforms.core import CombineGlobally
import apache_beam.transforms.window as window
from apache_beam.transforms import util
import googlemaps
from apache_beam.io.gcp.bigquery import parse_table_schema_from_json
from apache_beam.io.gcp import bigquery_tools
```

Posteriormente, se procederá a importar las librerías más comunes
```python
from datetime import datetime
import argparse
import json

```

A continuación se mostrarán las variables necesarias para poder ejecutar a lo largo del script

``` python
import loggiproject_id = "data-project-2-376316"
input_taxi_subscription = "taxi_position-sub"
input_user_subscription = "user_position-sub"
output_topic = "surge_pricing"
API_KEY = '######'
import requests
```

A modo de introducción es necesario tener una estuctura del flujo de datos que se va a realizar.

Para ello, nos vamos a apoyar en la siguiente imagen
![logo](https://cloud.google.com/static/pubsub/images/many-to-many.svg?hl=es-419)

Esto es conceptualmente lo que se va a ejecutar. Lo interesante es ver como se aplica a nuestro caso.

En la imagen siguiente quedan representadas las variables a utilizar y su posición en la arquitectura cloud.

INSERTAR IMAGEN CANVAShttps://www.canva.com/design/DAFX2RbheTY/fUMUeehecF2HyT7zmw4d1g/edit


A continuación se declara la primera función

``` python
def ParsePubSubMessage(message):
    #Decode PubSub message in order to deal with
    pubsubmessage = message.data.decode('utf-8')
    #Convert string decoded in json format(element by element)
    row = json.loads(pubsubmessage)
    #Logging
    logging.info("Receiving message from PubSub:%s", pubsubmessage)
    #Return function
    return row
```
Esta función hace referencia a la manera en la que se va a publicar los mensajes en PubSub

En primer lugar, se van covertir en unicode mediante ".decode(utf-8)" y se guardará en la varibale pubsubmessage

A continuación se cargan los mensajes en formato json mediante la función pubsubmessage

Finalmente, se realiza un "loggin.info" de manera que se notifique cada vez que se envia el mensaje para poder observar la evolución y detectar errores

```python
class MatchShortestDistance(beam.PTransform):

    def __init__(self):
        super().__init__()

    def expand(self, pcoll):
        return (pcoll
                | util.group_by_key()
                | util.Map(self._find_closest_match)
                )

    def _find_closest_match(self, group_key, group_values):
        
        # User leads the window
        User_id = group_key
        Taxi_id, distances = zip(*group_values)
        shortest_distance = min(distances)
        closest_taxi_index = distances.index(shortest_distance)

        return (User_id, (Taxi_id[closest_taxi_index], shortest_distance))
```

En este caso, se crea una clase que será hacer una coincidencia entre el usuario y el taxi más cercano.

Esto va a estar compuesto por incializar la clase, una función expand y finalmente la función "_find_closest_match"

En referencia a la funcion "expand", se divide en dos partes. Por un lado "util.group_by_key()" que agrupa a por clave valor, y por otro lado "util.Map(self._find_closest_match)" que mediane la función map hace un llamamiento a la función "_find_closest_match" para poder hacer la conincidencia del taxi más cercano y el cliente y devuelve finalmente una nueva pcollection.

A continuación es necesario porque se utiliza ".map" y ".ParDo". 

Básicamente se utiliza ".map" para tareas de procesamiento simples y secuenciales que no requieren un manejo complejo de los errores o las situaciones excepcionales.

En cambio ".ParDo" para tareas más complejas y que requieren un control más fino sobre el procesamiento de los elementos.

En nuestro caso se va a utilizar "ParDo" con el fin de tener un mayor control y por ello las clases serán creadas con "beam.DoFn" como veremos más adelante

