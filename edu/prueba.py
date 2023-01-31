import random
import time

    # Nombre de la función
def generate_random_position():
    # Inicializar la localizacion
    x = 0
    y = 0
    coordinates = [(x, y)]
    while True:
        # Hacer un movimiento aleatorio
        direction = random.choice(["N", "S", "E", "W"])
        if direction == "N":
            y += 0.0001
        elif direction == "S":
            y -= 0.0001
        elif direction == "E":
            x += 0.0001
        else:
            x-= 0.0001
        coordinates = [(x, y)]
        print(coordinates)
    
        time.sleep(3)

generate_random_position()
