import docker
import sys, getopt
import time
import os
import uuid
import random

print("#############################")
print("Starting Generator execution")
print("#############################")

topcontainers = 0
elapsedtime = 0
containername=""

containers=[]

def getcontainers():
    cmd=f"docker ps | grep -c {containername}"
    stream = os.popen(cmd)
    output = stream.read()
    return int(output)

def genuserid():
    return uuid.uuid4().hex

def deletecontainer(container_id):
    cmd=f"docker container rm {container_id} -f "
    stream = os.popen(cmd)
    output = stream.read()
    containers.remove(container_id)
    print(f"Container Removed with id: {container_id}")


def createcontainer():
    global containername
    global elapsedtime
    global topcontainers
    global containers
    userid=genuserid()
    cmd=f"docker run -e TIME_ID={elapsedtime} -e USER_ID={userid} -d {containername}:latest"
    stream = os.popen(cmd)
    output = stream.read().replace("\n","")
    containers.append(output)
    print(f"Container Created with id: {output} for user: {userid}")
    return output

def main(argv):
   global containername
   global elapsedtime
   global topcontainers
   try:
      opts, args = getopt.getopt(argv,"t:e:i:",["topcontainers=","elapsedtime=","imagename="])
   except getopt.GetoptError:
      print('main.py -t <topcontainers> -e <elapsedtime> -i <imagename>')
      sys.exit(2)
   for opt, arg in opts:
      if opt in ("-h", "--help"):
         print('main.py -t <topcontainers> -e <elapsedtime> -n <imagename>')
         print(" elapsedtime: int (seconds)")
         print(" topcotainers: int (top number of concurrent clients)")
         print(" image: string (image name)")
         sys.exit()
      elif opt in ("-t", "--topcontainers"):
         topcontainers = int(arg)
      elif opt in ("-e", "--elapsedtime"):
         elapsedtime = int(arg)
      elif opt in ("-i", "--image"):
         containername = arg
   print(f"Top Containers: {topcontainers}")
   print(f"Elapsed Time: {elapsedtime}")
   print(f"Container name: {containername}")

if __name__ == "__main__":
   main(sys.argv[1:])



while True:
   numcon=getcontainers()
   print(f"Currently running containers: {len(containers)}")
   if numcon<topcontainers:
    create=random.randint(0,topcontainers-numcon)
    print(f"Containers to be created: {create}")
    for i in range(0,create):
        createcontainer()
   else:
    print("No more containers can be created") 
   time.sleep(2)
   for item in containers:
    prob=random.randint(0, 10)
    if prob == 0:
        # 10% probabilidad de eliminar container
        deletecontainer(item)
    
   time.sleep(1)
