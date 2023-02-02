import google.auth
import requests
from google.cloud import bigquery
from google.oauth2.service_account import Credentials

# Autentificamos con BigQuery usando las credenciales de la cuenta de servicio
credentials = Credentials.from_service_account_file("path/to/service_account.json")
project_id = "data-project-2-376316"
client = bigquery.Client(credentials=credentials, project=project_id)

# Crea la consulta SQL para obtener las coordenadas de origen y destino desde BigQuery
query = """
SELECT Taxi_lat, taxi_lng, User_lat, User_lng
FROM 'our-dataset.your-table'
WHERE id = 1
"""

# Ejecuta la consulta
query_job = client.query(query)
rows = query_job.result()

# Especifica las coordenadas de origen y destino
origins = "row.Taxi_lat, row.Taxi_lng";
destinations = "row.User_lat, row.User_lng";

# Se introduce la API_KEY de Google Maps

API_KEY = 'AIzaSyBMazxFGKqM5rDVWyDiFSpESzqjLNgjY4U'  
url = "https://maps.googleapis.com/maps/api/distancematrix/json?origins={origins}&destinations={destinations}&key={API_KEY}"

# Reemplaza las coordenadas en la URL
url = url.format(origins=origins, destinations=destinations, API_KEY="{API_KEY}")

# Realiza una solicitud a la API de Google Maps
response = requests.get(url)

# Verifica si la solicitud es correcta
if response.status_code == 200:
    # Convierte la respuesta a un diccionario de Python
    data = response.json()

    # Obtiene la distancia en metros
    distance = data["rows"][0]["elements"][0]["distance"]["value"]

    # Imprime la distancia
    print("La distancia es de {} metros".format(distance))

else:
    # Imprime un mensaje de error si la solicitud falla
    print("Error al hacer una solicitud a la API de Google Maps")