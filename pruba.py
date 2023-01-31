def generatedata():
    import random
    from datetime import datetime, timedelta

    #Hora aleatoria
    def random_time():
        hour = random.randint(0, 23)
        minute = random.randint(0, 59)
        second = random.randint(0, 59)
        time = "{:02d}:{:02d}:{:02d}".format(hour, minute, second)
        print(time)

    data={}
    
    random_time()
    #data={}
    #data["time"]=random_time()

generatedata()