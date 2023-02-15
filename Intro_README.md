![Logo](https://n3m5z7t4.rocketcdn.me/wp-content/plugins/edem-shortcodes/public/img/logo-Edem.png)

# DATA-PROJECT 2
# 1. Caso de uso
## 1.1. Propuesta de proyecto
## 1.2. Aplicación de caso de uso
## 1.3. Elección On Premise or Cloud

# 2. Estructura de código
## 2.1. Generador de datos
### 2.1.1. Generador de datos taxi
### 2.2.2. Generador de datos usuario
## 2.2. Estructura Dataflow
## 2.3. Visualiazión Looker Studio

# 1. CASO DE USO
## 1.1. Propuesta de proyecto
"EDEM ha creado el día 18 de Marzo un evento de lanzamiento de empresas con productos IoT. Es vuestro momento! En este evento podréis presentar vuestro producto IoT como SaaS.


Durante estas tres semanas, debéis pensar un producto IoT, desarrollarlo y simular su uso.

![Logo](https://revistabyte.es/wp-content/uploads/2021/03/nueva-colaboracion-de-telefonica-para-ofrecer-soluciones-iot-integradas-con-blockchain.jpg)

Este proyecto debe tener una arquitectura escalable pero no es obligatorio cloud, vosotros decidís.

De cara a participar en este evento, vuestra solución debe ser escalable, opensource y  cloud."

![Logo](https://www.muycomputerpro.com/wp-content/uploads/2018/02/google-iot-core.png)

## 1.2. Aplicación de caso de uso
El objetivo es la creación de un Saas de manera que los usuarios puedan pedir un taxi en cualquier momento, informando del precio y la ruta.

![Logo](https://img.freepik.com/vector-premium/gente-pide-taxi-linea-calle_160308-4540.jpg?w=2000)

En todo momento, se recibe señal de los taxis mediante el id, su método de pago, su posición actual, estado (si está ocupado o libre) siempre en tiempo real

A su vez, se recibe señal de los clientes siempre y cuando estos soliciten el servicio. Se recibirá la localización incial, es decir, dónde está ubicado el cliente para que el taxi pueda recogerle. El momento en que lo reserva en cuanto a tiempo, y finalmente si ha sido recogido.


# 1.3. Elección: On Premise o Cloud
## Parte 01: Elección si On premise o cloud
En nuestro caso, se ha optado por la elección en Cloud. A continuación se mostrarán los motivos.

### Picos de demanda

El motivo principal es el hecho, de que en fechas claves del año, ya sea Navidad, Nochevieja, Fallas en Valencia o las Hogueras de San Juan, lo más seguro es que ocurra una alta demanda y sería indispensable recurrir a incorporar más equipos físicos solo para estas fechas.

![logo](https://st2.depositphotos.com/1005979/6864/i/450/depositphotos_68642207-stock-photo-more-words-in-gears-and.jpg)

### Términos económicos

La diferencia respecto on Premise se basrá en una variable que posee el Cloud Compunting, y es la elasticidad y a continuación se explicará por qué.

En caso de "On Premise", em términos económicos a priori si se prevee invertir en servidores por la previsión del pico de demanda, sería un gasto y no se obtendria un ROI apropiado para la empresa, incluso pudiéndose dar el caso de acometer pérdidas. Esto es debido, a que no se está utilizando un rendimiento adecuado fuera de estas fechas entre otros.

![logo](https://img.freepik.com/vector-premium/perdida-dinero-efectivo-grafico-acciones-flecha-abajo-concepto-crisis-financiera-caida-mercado-bancarrota-ilustracion-stock_100456-1703.jpg)

En cambio, si se opta por el "Cloud computing", se puede aprovechar la elasticidad que este servicio ofrece.

La elasticidad es la capacidad de adaptarse a la demanda del usuario (en este caso la empresa que utiliza el servicio cloud). 

A su vez, es destacable en cuanto a recursos económicos, ya que solamente se utilizará la cantidad de recursos necesaria en cada momento necesario, pagando así únicamente por lo que se usa.

En el siguiente gráfico se muestra el comportamiento de la elasticidad.

![logo](https://aitor-medrano.github.io/bigdata2122/imagenes/cloud/01costOportunity.png)

### Tolerancia a fallos

Por último cabe mencionar un aspecto muy importante, y es la tolerancia a fallos, es decir, la probabildiad de que el conjunto de servidores fallen.

En el caso de optar por una estrategia "On Premise", es más probable que pueda ocurrir una caida del servidor. Esto se peude deber a diversos motivos

En cambio si se escoge "Cloud", se está escogiendo un servidor con una tasa de fallos prácticamente nula por lo que es más segura.

También existen otros beneficios como se pueden apreciar en la imagen siguiente que no se comentan pero que apoyan la decisión de realizarla "On Cloud"

![logo](https://d3t4nwcgmfrp9x.cloudfront.net/upload/VENTAJAS-CLOUD.jpg)

# 2. Estructura de código
## 2.1. Generador de datos
En este apartado se va a proceder a explicar cómo se van a generar los datos de taxi y de usuario con el fin de mandarlos finalmente a cada uno su TOPIC en PUBSUB.

### 2.1.1. Generador de datos taxi
Mediante una imagen docker se creará el contenedor generador de taxis, donde será incluido el script que se encuentra en data_project_2\main\python\main.py para crear los generadores de taxi.

```python
python3 main.py -t 10 -e 2 -i [nombre_imagen]
```

Por cada conductor de taxi, se obtendrán las variables: zone_id, ubicación del taxi delimitado por taxi_lat y taxi_lng, precio del taxi mediante taxibase_fare y taxikm_fare.

### 2.2.2. Generador de datos usuario
De la misma forma que se ha hecho con el taxi, se hará con usuario.

Por cada usuario se obtendrán las variables: zone_id, user_id, user_name, user_phone_number,user_email,userinit_lat,userinit_lng,userfinal_lat,userfinal_lng.

## 2.2. Estructura Dataflow
- Se leen los datos de los dos topics, taxi y usuarios.
- Juntamos tanto los datos de taxi como de usuario en una misma pcollection con CoGroupByKey.
- Para asociar el taxi al usario, se realizará por orden de disponibilidad de taxis.
- Se carga el esquema de big query para que se cree la tabla.
- Escribimos los datos en bigquery.

## 2.3. Visualiazión Looker Studio