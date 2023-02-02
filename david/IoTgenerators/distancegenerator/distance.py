import google.auth
import requests
from google.cloud import bigquery
from google.oauth2.service_account import Credentials

# Autentica con BigQuery usando las credenciales de la cuenta de servicio
credentials = Credentials.from_service_account_file("path/to/service_account.json")
project_id = "our-project-id"
client = bigquery.Client(credentials=credentials, project=project_id)

# Define la consulta SQL para obtener las coordenadas de origen y destino desde BigQuery
query = """
SELECT origin_lat, origin_lng, dest_lat, dest_lng
FROM 'our-dataset.your-table'
WHERE id = 1
"""

# Ejecuta la consulta
query_job = client.query(query)
rows = query_job.result()

# Obtiene las coordenadas de origen y destino
origin = [row.origin_lat, row.origin_lng]
destination = [row.dest_lat, row.dest_lng]

####################################################################################################

# Introducimos API_KEY de Google Maps

API_KEY = 'AIzaSyBMazxFGKqM5rDVWyDiFSpESzqjLNgjY4U'  

url = "https://maps.googleapis.com/maps/api/distancematrix/json?origins={origins}&destinations={destinations}&key={API_KEY}"

# Especifica las coordenadas de origen y destino
origins = "row.origin_lat, row.origin_lng";
destinations = "row.dest_lat, row.dest_lng";

# Reemplaza las coordenadas en la URL
url = url.format(origins=origins, destinations=destinations, API_KEY="{API_KEY}")

# Realiza una solicitud a la API de Google Maps
response = requests.get(url)

# Verifica si la solicitud es exitosa
if response.status_code == 200:
    # Convierte la respuesta a un diccionario de Python
    data = response.json()
    # Obtén la distancia en metros
    distance = data["rows"][0]["elements"][0]["distance"]["value"]
    # Imprime la distancia
    print("La distancia es de {} metros".format(distance))
else:
    # Imprime un mensaje de error si la solicitud falla
    print("Error al hacer una solicitud a la API de Google Maps")