import google.auth
import requests
from google.cloud import bigquery
from google.oauth2.service_account import Credentials

# Autentificar con BigQuery usando las credenciales de la cuenta de servicio
credentials = Credentials.from_service_account_file("path/to/service_account.json")
project_id = "data-project-2-376316"
client = bigquery.Client(credentials=credentials, project=project_id)

# Crea la consulta SQL para obtener coordenadas de ubicacion del taxi, ubicacion del usuario y destino final desde BigQuery
query = """
SELECT Taxi_lat, Taxi_lng, Userinit_lat, Userinit_lng, Userfinal_lat, Userfinal_lng,
FROM 'our-dataset.your-table'
WHERE id = 1
"""

# Ejecuta la consulta
query_job = client.query(query)
rows = query_job.result()

# Especifica las coordenadas de origen, ubicacion usuario y destino
origin = "row.Taxi_lat, row.Taxi_lng"
destinationinit = "row.Userinit_lat, row.Userinit_lng"
destinationfinal = "row.Userfinal_lat, row.Userfinal_lng"

# Se introduce la API_KEY de Google Maps

API_KEY = 'AIzaSyBMazxFGKqM5rDVWyDiFSpESzqjLNgjY4U'  
urlinit = "https://maps.googleapis.com/maps/api/distancematrix/json?origins={origin}&destinations={destinationinit}&key={API_KEY}"
urlfinal = "https://maps.googleapis.com/maps/api/distancematrix/json?origins={destinationinit}&destinations={destinationfinal}&key={API_KEY}"

# Reemplaza las coordenadas en las URLs para la posicion del taxi hasta la posicion de usuario y destino final
urlinit = urlinit.format(origin=origin, destinationinit=destinationinit, API_KEY="{API_KEY}")
urlfinal = urlfinal.format(destinationinit=destinationinit, destinationfinal=destinationfinal, API_KEY="{API_KEY}")

# Realiza una solicitud a la API de Google Maps
responseinit = requests.get(urlinit)
responsefinal = requests.get(urlfinal)

# Verifica si la solicitud es correcta
if responseinit.status_code == 200:
    # Convierte las respuesta a un diccionario de Python
    datainit = responseinit.json()

    # Obtiene la distancia en metros al usuario
    Distance_user = datainit["rows"][0]["elements"][0]["distance"]["value"]

    # Imprime la distancia al usuario
    print("La distancia a la posicion del usuario es de {} metros".format(distance_user))

else:
    # Imprime un mensaje de error si la solicitud falla
    print("Error al hacer una solicitud de la posicion del usuario a la API de Google Maps")

# Verifica si la solicitud es correcta
if responsefinal.status_code == 200:
    # Convierte las respuesta a un diccionario de Python
    datafinal = responsefinal.json()

    # Obtiene la distancia en metros al destino
    Distance_destination = datafinal["rows"][0]["elements"][0]["distance"]["value"]

       # Imprime la distancia al destino desde la posicion del usuario
    print("La distancia a la posicion del usuario al destino final es de {} metros".format(distance_destination))

else:
    # Imprime un mensaje de error si la solicitud falla
    print("Error al hacer una solicitud del destino final del usuario a la API de Google Maps")

# Obtiene la distancia total en metros
Distance_total = Distance_user + Distance_destination

# Imprime la distancia al total
print("La distancia total es de {} metros".format(Distance_total))