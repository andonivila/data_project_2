import googlemaps
import os
from google.cloud import bigquery
from google.oauth2.service_account import Credentials

# Autentificar con BigQuery usando las credenciales de la cuenta de servicio
#credentials = Credentials.from_service_account_file("./dataflow/data-project-2-376316-6817462f9a56.json")
#project_id = "data-project-2-376316"
#lient = bigquery.Client(credentials=credentials, project=project_id)

'''
CHEQUEAR QUE OPCION ES VALIDA UNA VEZ CONECTEMOS CON BIGQUERY
credentials = os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="data-project-2-376316-6817462f9a56.json" 
'''
'''
# Crea la consulta SQL para obtener coordenadas de ubicacion del taxi, ubicacion del usuario y destino final desde BigQuery
query = """
SELECT Taxi_lat, Taxi_lng, Userinit_lat, Userinit_lng, Userfinal_lat, Userfinal_lng,
FROM 'our-dataset.your-table'
WHERE id = 1
"""


# Ejecuta la consulta
query_job = client.query(query)
rows = query_job.result()
'''

# Prueba introduccion datos posicion
Taxi_lat = "39.49812487550294"
Taxi_lng = "-0.3436109452203315"
Userinit_lat = "39.46674308189089"
Userinit_lng = "-0.3545383831836688"
Userfinal_lat = "39.50394308189089"
Userfinal_lng = "-0.3865383831836688"

# Especifica las coordenadas de origen, ubicacion usuario y destino
taxi_position = Taxi_lat, Taxi_lng
user_position  = Userinit_lat, Userinit_lng
user_destination = Userfinal_lat, Userfinal_lng

# Introducir la API_KEY de Google Maps
API_KEY = 'AIzaSyBMazxFGKqM5rDVWyDiFSpESzqjLNgjY4U'

# Realiza una solicitud a la API de Google Maps
gmaps = googlemaps.Client(key=API_KEY) 

# Obtiene la distancia en metros al usuario y la imprime
resultinit = gmaps.distance_matrix(taxi_position, user_position, mode='driving')["rows"][0]["elements"][0]['distance']["value"]
print('El trayecto del taxi a la posicion del usuario es:',resultinit, 'mts')

# Obtiene la distancia en metros al destino y la imprime
resultfinal = gmaps.distance_matrix(user_position, user_destination, mode='driving')["rows"][0]["elements"][0]['distance']["value"]
print('El trayecto desde el usuario al destino es:',resultfinal, 'mts')

# Obtiene la distancia total en metros y la imprime
Total_distance = resultinit + resultfinal
print('El trayecto total es:',Total_distance, 'mts')