from googlemaps import Client
import os
from dotenv import load_dotenv


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

load_dotenv()

# Introducir la API_KEY de Google Maps
clv_gm = str(os.environ['CLAVE_API_GOOGLE_MAPS'])

# Realiza una solicitud a la API de Google Maps
gmaps = Client(key=clv_gm) 

# Obtiene la distancia en metros al usuario y la imprime
resultinit = gmaps.distance_matrix(taxi_position, user_position, mode='driving')["rows"][0]["elements"][0]['distance']["value"]
print('El trayecto del taxi a la posicion del usuario es:',resultinit, 'mts')

# Obtiene la distancia en metros al destino y la imprime
resultfinal = gmaps.distance_matrix(user_position, user_destination, mode='driving')["rows"][0]["elements"][0]['distance']["value"]
print('El trayecto desde el usuario al destino es:',resultfinal, 'mts')

# Obtiene la distancia total en metros y la imprime
Total_distance = resultinit + resultfinal
print('El trayecto total es:',Total_distance, 'mts')