import random
import time



def my_rando ():
    my_lat_g = 39
    my_lat_m = random.randint(27,29)/60
    my_lat_s = random.uniform(55,00)/3600
    my_f_lat = str (my_lat_g+my_lat_m+my_lat_s)

    my_long_g=0
    my_long_m=random.randint(20,25)/60
    my_long_s= random.uniform(60,00)/3600
    my_f_long= str(my_long_g+my_long_m+my_long_s)
    my_f_long = (f"- {my_f_long}")

    coordenadas= (my_f_lat,my_f_long)
    

    print(coordenadas)
    





my_rando()


