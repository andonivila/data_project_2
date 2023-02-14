""" Serverless Data Processing with Dataflow
    Master Data Analytics EDEM
    Academic Year 2022-2023"""

""" Solución de contingencia por 
    si no nos funciona el otro main """

""" Import libraries """

#Import beam libraries
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.combiners import MeanCombineFn
from apache_beam.transforms.combiners import CountCombineFn
from apache_beam.transforms.core import CombineGlobally
import apache_beam.transforms.window as window
from apache_beam.transforms import util
import googlemaps

from apache_beam.io.gcp.bigquery import parse_table_schema_from_json
from apache_beam.io.gcp import bigquery_tools

#Import Common Libraries
import argparse
import json
import logging
import requests

#Initial variables
project_id = "data-project-2-376316"
input_taxi_subscription = "taxi_position-sub"
input_user_subscription = "user_position-sub"
output_topic = "surge_pricing"
output_big_query_user = "dataproject2376316.user_table"
output_big_query_taxi = "dataproject2376316.taxi_table"
bigquery_schema_path_user = "schema/bq_user.json"
bigquery_schema_path_taxi = "schema/bq_taxi.json"

#Indicamos clave Google Maps
clv_gm = 'AIzaSyBMazxFGKqM5rDVWyDiFSpESzqjLNgjY4U'

'''Functions'''
def ParsePubSubMessage(message):
    #Decode PubSub message in order to deal with
    pubsubmessage = message.data.decode('utf-8')

    #Convert string decoded in json format(element by element)
    row = json.loads(pubsubmessage)

    #Logging
    logging.info("Receiving message from PubSub:%s", pubsubmessage)

    #Return function
    return row

# def fill_none(element, default_value):
#     if element is None:
#         return default_value
#     return element

#DoFn01: Add processing timestamp
class AddTimestampDoFn(beam.DoFn):

    #Process function to deal with data
    def process(self, element):
        from datetime import datetime

        #Add Processing time field
        element['processing_time'] = str(datetime.now())
        yield element

##DoFn02: Extract payload
class extractPayloadDoFn(beam.DoFn):
    def process(self, element):
        yield element[1]

#DoFn02: Get the location fields
class getLocationsDoFn(beam.DoFn):
    def process(self, element):
        
        yield (
            element['taxi_id'],
            element['taxi_lat'], 
            element['taxi_lng'], 
            element['taxibase_fare'], 
            element['taxikm_fare'],
            element['user_id'], 
            element['userinit_lat'], 
            element['userinit_lng'], 
            element['userfinal_lat'], 
            element['userfinal_lng'], 
            )


#DoFn03: Calculate distance between user init location and taxi
class CalculateInitDistancesDoFn(beam.DoFn):
    def process(self, element):

        taxi_lat = element['taxi_lat']
        taxi_long = element['taxi_lng']
        user_init_lat = element['userinit_lat']
        user_init_long = element['userinit_lng']

        taxi_position = taxi_lat, taxi_long
        user_intit_position = user_init_lat, user_init_long

        # Realiza una solicitud a la A.P.I. de Google Maps
        gmaps = googlemaps.Client(key=clv_gm) 

        # Accedemos al elemento distance del JSON recibido
        element['init_distance'] = gmaps.distance_matrix(taxi_position, user_intit_position, mode='driving')["rows"][0]["elements"][0]['distance']["value"]

        yield element

#DoFn04: Calculate final distance between user init location and user final location
class CalculateFinalDistancesDoFn(beam.DoFn):
    def process(self, element):

        # credentials = Credentials.from_service_account_file("./dataflow/data-project-2-376316-6817462f9a56.json")
        user_init_lat = element['userinit_lat']
        user_init_long = element['userinit_lng']
        user_final_lat = element['Userfinal_lat']
        user_final_long = element['Userfinal_lng']

        
        user_intit_position = user_init_lat, user_init_long
        user_destination = user_final_lat, user_final_long

        # Realiza una solicitud a la API de Google Maps
        gmaps = googlemaps.Client(key=clv_gm) 

        # Accedemos al elemento distance del JSON rebido
        element['final_distance'] = gmaps.distance_matrix(user_destination, user_intit_position, mode='driving')["rows"][0]["elements"][0]['distance']["value"]

        yield element

#DoFn05: Removing locations from data once init and final distances are calculated
class RemoveLocations(beam.DoFn):
    def process(self, element):
        yield (
            element['user_id'], 
            element['taxi_id'], 
            element['init_distance'], 
            element['final_distance']
        )


#DoFn06: Calculating final distance
class AddFinalDistanceDoFn(beam.DoFn):
    def process(self, element):
        element['total_distance'] = element['init_distance'] + element['final_distance']

        yield element

# class CalculateTransactionAmount(beam.DoFn):
#     def process(self, element):


'''PTransform Classes'''
 
# class MatchShortestDistance(beam.PTransform):
#     def expand(self, pcoll):
#         match = (pcoll
#                 |"Add Processing Time" >> beam.ParDo(AddTimestampDoFn())
#                 |"Set fixed windows each 30 secs" >> beam.WindowInto(window.FixedWindows(60))
#                 |"Group by timestamp" >> beam.GroupByKey()
#                 |"Get locations" >> beam.ParDo(getLocationsDoFn())
#                 |"Call Google maps API to calculate distances between user and taxis" >> beam.ParDo(CalculateInitDistancesDoFn())
#                 |"Call Google maps API to calculate distances between user_init_loc and user_final_loc" >> beam.ParDo(CalculateFinalDistancesDoFn())
#                 |"Key by user_id" >> beam.Map(lambda x: (x['user_id'], x))
#                 |"Group by user_id" >> beam.GroupByKey()
#                 | "Find shortest distance" >> beam.Map(lambda x: {
#                     'user_id': x[0],
#                     #Aqui podemos ir sacando los campos que queramos de la PColl inicial
#                     'taxi_id': min(x[1], key=lambda y: y['init_distance'])['taxi_id'],
#                     'calculate shortest_distance': min(x[1], key=lambda y: y['init_distance'])['init_distance'],
#                     'calculate final distance': min(x[1], key=lambda y: y['init_distance'])['final_distance']
#                 })
#             )

#         return match

class GroupMessagesByFixedWindows(beam.PTransform):
    """A composite transform that groups Pub/Sub messages based on publish time
    and outputs a list of tuples, each containing a message and its publish time.
    """
    def __init__(self, window_size, num_shards=5):
        # Set window size to 30 seconds.
        self.window_size = int(window_size * 30)
        self.num_shards = num_shards

    def expand(self, pcoll):
        return (
            pcoll
            # Bind window info to each element using element timestamp (or publish time).
            | "Window into fixed intervals">> beam.WindowInto(window.FixedWindows(self.window_size))
            #| "Add timestamp to windowed elements" >> beam.ParDo(AddTimestampDoFn())
                                    
        )
        
    
'''Dataflow Process'''
def run_pipeline(window_size = 1, num_shards = 5):

    parser = argparse.ArgumentParser(description=('Arguments for the Dataflow Streaming Pipeline'))

    args, pipeline_opts = parser.parse_known_args()

    #Load schema from /schema folder 
    with open(bigquery_schema_path_user) as file1:
            input_schema = json.load(file1)

    user_schema = bigquery_tools.parse_table_schema_from_json(json.dumps(input_schema))

    with open(bigquery_schema_path_taxi) as file2:
            input_schema = json.load(file2)

    taxi_schema = bigquery_tools.parse_table_schema_from_json(json.dumps(input_schema))



    ### Apache Beam Pipeline
    #Pipeline options
    options = PipelineOptions(pipeline_opts, save_main_session = True, streaming = True, project = project_id)

    #Pipeline
    with beam.Pipeline(argv=pipeline_opts, options=options) as p:

        ###Step01: Read user and taxi data from PUB/SUB
        user_data = (
            p 
                |"Read User data from PubSub" >> beam.io.ReadFromPubSub(subscription=f"projects/{project_id}/subscriptions/{input_user_subscription}", with_attributes = True)
                |"Parse User JSON messages" >> beam.Map(ParsePubSubMessage)
                |"Window into data" >> GroupMessagesByFixedWindows(window_size, num_shards)
                | "Write user to BigQuery" >> beam.io.WriteToBigQuery(
                    table = f"{project_id}:{output_big_query_user}",
                    schema = user_schema,
                    create_disposition = beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                    write_disposition = beam.io.BigQueryDisposition.WRITE_APPEND
                )
            )


        taxi_data = (
            p
                |"Read Taxi data from PubSub" >> beam.io.ReadFromPubSub(subscription=f"projects/{project_id}/subscriptions/{input_taxi_subscription}", with_attributes = True)
                |"Parse Taxi JSON messages" >> beam.Map(ParsePubSubMessage)
                |"Window into taxi" >> GroupMessagesByFixedWindows(window_size, num_shards)
                | "Write taxi to BigQuery" >> beam.io.WriteToBigQuery(
                    table = f"{project_id}:{output_big_query_taxi}",
                    schema = taxi_schema,
                    create_disposition = beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                    write_disposition = beam.io.BigQueryDisposition.WRITE_APPEND
                )
        )

        ###Step02: Merge Data from taxi and user topics into one PColl
        # Here we have taxi and user data in the same  table

        # data = (
        #     {"taxis": taxi_data, "users": user_data} | beam.CoGroupByKey()
        #     |"Extract Payload" >> beam.ParDo(extractPayloadDoFn())
        #     |
        #     #|"Add timestamp" >> beam.ParDo(AddTimestampDoFn())
        #     #|"Get shortest distance between user and taxis" >> MatchShortestDistance()
        # )

        

        ###Step05: Get the closest driver for the user per Window
        # (
        #     data
        #          |"Get location fields." >> beam.ParDo(getLocationsDoFn())
        #          |"Call Google maps API to calculate distances between user and taxis" >> beam.ParDo(CalculateInitDistancesDoFn())
        #          |"Call Google maps API to calculate distances between user_init_loc and user_final_loc" >> beam.ParDo(CalculateFinalDistancesDoFn())
        #          |"Removing locations from data once init and final distances are calculated" >> beam.ParDo(RemoveLocations()) 
        #          |"Set fixed window" >> beam.WindowInto(window.FixedWindows(60))
        #          |"Get shortest distance between user and taxis" >> MatchShortestDistance()
        #          |"Calculate total distance" >> beam.ParDo(AddFinalDistanceDoFn())
        #          |"Write to BigQuery" >> beam.io.WriteToBigQuery(
        #             table = f"{project_id}:{args.output_bigquery}",
        #             schema = schema,
        #             create_disposition = beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        #             write_disposition = beam.io.BigQueryDisposition.WRITE_APPEND
        #         )
                 #|"Calculate transaction amount" >> beam.ParDo(CalculateTransactionAmount())
        # )
        

if __name__ == '__main__' : 
    #Add Logs
    logging.getLogger().setLevel(logging.INFO)
    #Run process
    run_pipeline()
