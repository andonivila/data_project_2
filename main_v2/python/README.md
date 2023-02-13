# dp2_script
Initial code for DP 2 in MDA

Este reto se basa en ser capaces de construir un prototipo para un dispositivo IoT. El framework contenido en este repositorio debe servir para simular la población de vuestro producto.

Para ello disponéis de las siguientes partes

## Cliente

Este contenedor tendrá la lógica de un cliente de vuestra aplicación y será el encargado de generar sus datos ficticios y enviarlos a vuestro sistema de mensajería que hayáis decidido. El código se encuentra dentro de la carpeta **client**. Para usarlo, deberéis generar un contenedor cliente y darle un nombre que usaréis más adelante.

## Generador de Clientes

Este script main.py simula la existencia de multiples clientes enviando datos. Para ello generará aleatoriamente un numero de contenedores de tipo cliente que enviarán datos.

Para usar este script debéis ejecutar:

```
main.py -t <topcontainers> -e <elapsedtime> -i <imagename>
```
ejemplo:
Creamos un máximo de 100 contenedores y que envíe datos cada 2 segundos un contenedor llamado client

```
main.py -t 100 -e 2 -i client

```


Recibiréis un output como el siguiente:

```
########################
Starting Generator execution
#############################
Top Containers: 7
Elapsed Time: 1
Container name: client
Currently running containers: 0
Containers to be created: 4
Container Created with id: 61ce7cf048f8cde24a21f28cc98fe513fe2a63089ad750c0675394393dd748bb for user: 158377ae53944658b6ea2aa82aa9e0e7
Container Created with id: 3db60b0903e23ea8498ea91cc83fc61c1c11d631a5d7f48c121a5c1c240c9bcc for user: 9516b4aa460e47ccb0a0b3cc3ee3d88f
Container Created with id: c87dda5e856e29e234b3ff7c243568fc76c834d522e13f238707c7dd980b9593 for user: 6a0ef69eb7d3439195ab0f14f1746613
Container Created with id: c497013d7f0c77eb03e6d966aea2af7687c0e0c3e85bb5fab92d4a4261a42793 for user: fa8f500274024984aec82963067adf7a
Container Removed with id: 3db60b0903e23ea8498ea91cc83fc61c1c11d631a5d7f48c121a5c1c240c9bcc
```

