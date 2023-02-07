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

## Clases 
```python
class AddTimestampDoFn(beam.DoFn):

    #Process function to deal with data
    def process(self, element):
        #Add Processing time field
        element['Processing_Time'] = str(datetime.now())
        yield element
``` 
En este caso la función hara referencia a que se modificará el type de la variable element en su columna "Porcessing time" por una de carácter string

```python
#DoFn02: Get the location fields
class getLocationsDoFn(beam.DoFn):
    def process(self, element):
        
        yield element['taxi_id', 'taxi_lat', 'taxi_lng', 'user_id', 'userinit_lat', 'userinit_lng', 'userfinal_lat', 'userfinal_lng']
```

Mediante esta función, nos devolvería cual sería la posición geográfica tanto del taxi como del usario

``` python
class CalculateInitDistancesDoFn(beam.DoFn):
    def process(self, element):

        taxi_lat = element['taxi_lat']
        taxi_long = element['taxi_lng']
        user_init_lat = element['userinit_lat']
        user_init_long = element['userinit_lng']

        taxi_position = taxi_lat, taxi_long
        user_intit_position = user_init_lat, user_init_long

        # Realiza una solicitud a la A.P.I. de Google Maps
        gmaps = googlemaps.Client(key=clv_gm) 

        # Accedemos al elemento distance del JSON recibido
        element['init_distance'] = gmaps.distance_matrix(taxi_position, user_intit_position, mode='driving')["rows"][0]["elements"][0]['distance']["value"]

        yield element
```

Con esta función conseguiremos calcular cuál es la distancia entre el usuairo y el taxi. Más adelante se verá como no es únicamente medirá la distancia de un usuario a un taxi, si no que recogerá la ubicación de todos los taxis y calculará la distancia con la API de Google Maps que es la siguiente clase:

``` python
class CalculateFinalDistancesDoFn(beam.DoFn):
    def process(self, element):

        # credentials = Credentials.from_service_account_file("./dataflow/data-project-2-376316-6817462f9a56.json")
        user_init_lat = element['userinit_lat']
        user_init_long = element['userinit_lng']
        user_final_lat = element['Userfinal_lat']
        user_final_long = element['Userfinal_lng']

        
        user_intit_position = user_init_lat, user_init_long
        user_destination = user_final_lat, user_final_long

        # Realiza una solicitud a la API de Google Maps
        gmaps = googlemaps.Client(key=clv_gm) 

        # Accedemos al elemento distance del JSON rebido
        element['final_distance'] = gmaps.distance_matrix(user_destination, user_intit_position, mode='driving')["rows"][0]["elements"][0]['distance']["value"]

        yield element
```
Una vez se haya calculado la distancia y guardada como una clave más, se procederá a eliminar las posiciones geográficas de los taxis, así como los usuarios ya que no son necearias

De esa manera conseguiremos obtener finalmente en "BigQuery" únicamente los datos realmente útiles.

``` python
class RemoveLocations(beam.DoFn):
    def process(self, element):
        yield element['user_id', 'taxi_id', 'init_distance', 'final_distance']
```

## DATAFLOW PROCESS
```python
def run_pipeline():

    # Input arguments
    parser = argparse.ArgumentParser(description=('Arguments for the Dataflow Streaming Pipeline'))

    parser.add_argument('--output_bigquery', required=True, help='Table where data will be stored in BigQuery. Format: <dataset>.<table>.')
    parser.add_argument('--bigquery_schema_path', required=True, help='BigQuery Schema Path within the repository.')

    args, pipeline_opts = parser.parse_known_args()

    #Load schema from /schema folder 
    with open(args.bigquery_schema_path) as file:
            input_schema = json.load(file)

    schema = bigquery_tools.parse_table_schema_from_json(json.dumps(input_schema))

    ### Apache Beam Pipeline
    #Pipeline options
    options = PipelineOptions(pipeline_opts, save_main_session = True, streaming = True, project = project_id)

    #Pipeline
    with beam.Pipeline(argv=pipeline_opts, options=options) as p:

        ###Step01: Read user and taxi data from PUB/SUB
        user_data = (
            p 
                |"Read User data from PubSub" >> beam.io.ReadFromPubSub(subscription=f"projects/{project_id}/subscriptions/{input_user_subscription}", with_attributes = True)
                |"Parse User JSON messages" >> beam.Map(ParsePubSubMessage)
                |"Add User Processing Time" >> beam.ParDo(AddTimestampDoFn())
        )

        taxi_data = (
            p
                |"Read Taxi data from PubSub" >> beam.io.ReadFromPubSub(subscription=f"projects/{project_id}/subscriptions/{input_taxi_subscription}", with_attributes = True)
                |"Parse Taxi JSON messages" >> beam.Map(ParsePubSubMessage)
                |"Add Taxi Processing Time" >> beam.ParDo(AddTimestampDoFn())
        )

        ###Step02: Merge Data from taxi and user topics into one PColl
        # Here we have taxi and user data in the same  table
        data = (user_data, taxi_data) | beam.Flatten()

        ###Step05: Get the closest driver for the user per Window
        (
            data 
                 |"Get location fields." >> beam.ParDo(getLocationsDoFn())
                 |"Call Google maps API to calculate distances between user and taxis" >> beam.ParDo(CalculateFinalDistancesDoFn())
                 |"Call Google maps API to calculate distances between user_init_loc and user_final_loc" >> beam.ParDo(CalculateFinalDistancesDoFn())
                 |"Removing locations from data once init and final distances are calculated" >> beam.ParDo(RemoveLocations()) 
                 |"Set fixed window" >> beam.WindowInto(window.FixedWindows(60))
                 |"Get shortest distance between user and taxis" >> MatchShortestDistance()
         )



         ###Step06: Write combined data to BigQuery
        (
            data | "Write to BigQuery" >> beam.io.WriteToBigQuery(
                table = f"{project_id}:{args.output_bigquery}",
                schema = schema,
                create_disposition = beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition = beam.io.BigQueryDisposition.WRITE_APPEND
            )
        )
        

```
Este código es un ejemplo de un pipeline de Apache Beam para procesar datos en tiempo real. Se trata de un pipeline de flujo de datos en tiempo real para integrar datos de usuarios y taxis.

Linea por linea:
```python
def run_pipeline(): #se define una función llamada run_pipeline que ejecutará el pipeline de Apache Beam.
```

```python
parser = argparse.ArgumentParser(description=('Arguments for the Dataflow Streaming Pipeline'))

#Se crea un analizador de argumentos utilizando la biblioteca argparse para obtener los argumentos necesarios para el pipeline.

```

```python
parser.add_argument('--output_bigquery', required=True, help='Table where data will be stored in BigQuery. Format: <dataset>.<table>.') 

#Se agrega un argumento llamado output_bigquery que es obligatorio y proporciona la tabla de destino en BigQuery donde se almacenarán los datos. El formato de la tabla es <dataset>.<table>.
```
```python
parser.add_argument('--bigquery_schema_path', required=True, help='BigQuery Schema Path within the repository.') 

#Se agrega otro argumento llamado bigquery_schema_path que es obligatorio y proporciona la ruta del esquema de BigQuery dentro del repositorio.
```

``` python
args, pipeline_opts = parser.parse_known_args() 

#Se llama a parser.parse_known_args() para obtener los argumentos conocidos y las opciones del pipeline.
```

```python
with open(args.bigquery_schema_path) as file: 
    
#Se abre el archivo con la ruta especificada por el argumento bigquery_schema_path.
```

``` python
input_schema = json.load(file) 

#Se carga el esquema en formato JSON del archivo.
```


``` python
schema = bigquery_tools.parse_table_schema_from_json(json.dumps(input_schema)) 

S#e procesa el esquema utilizando la función parse_table_schema_from_json de la biblioteca bigquery_tools.
```
``` python
options = PipelineOptions(pipeline_opts, save_main_session = True, streaming = True, project = project_id) 

#Se crean las opciones del pipeline especificando que se trata de un pipeline en tiempo real y el ID del proyecto.
```
``` python
with beam.Pipeline(argv=pipeline_opts, options=options) as p: 

#Se inicia el pipeline de Apache Beam.
```
``` python
user_data = (p |"Read User data from PubSub" >> beam.io.ReadFromPubSub(subscription=f"projects/{project_id}/subscriptions/{input_user_subscription}", with_attributes = True) 

#Se crea una PCollection llamada
```