""" Serverless Data Processing with Dataflow
    Master Data Analytics EDEM
    Academic Year 2022-2023"""

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
from datetime import datetime
import argparse
import json
import logging
import requests

#Initial variables
project_id = "data-project-2-376316"
input_taxi_subscription = "taxi_position-sub"
input_user_subscription = "user_position-sub"
output_topic = "surge_pricing"

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


'''PTransform Classes'''
 
class MatchShortestDistance(beam.PTransform):
    def expand(self, pcoll):
        match = (pcoll
                |"Key by user_id" >> beam.Map(lambda x: (x['user_id'], x))
                |"Group by user_id" >> beam.GroupByKey()
                | "Find shortest distance" >> beam.Map(lambda x: {
                    'user_id': x[0],
                    #Aqui podemos ir sacando los campos que queramos de la PColl inicial
                    'taxi_id': min(x[1], key=lambda y: y['init_distance'])['taxi_id'],
                    'calculate shortest_distance': min(x[1], key=lambda y: y['init_distance'])['init_distance'],
                    'calculate final distance': min(x[1], key=lambda y: y['init_distance'])['final_distance']
                })
            )

        return match
        


'''DoFn Classes'''

#DoFn01: Add processing timestamp
class AddTimestampDoFn(beam.DoFn):

    #Process function to deal with data
    def process(self, element):
        #Add Processing time field
        element['processing_time'] = str(datetime.now())
        
        yield element

#DoFn02: Get the location fields
class getLocationsDoFn(beam.DoFn):
    def process(self, element):
        
        yield element['taxi_id', 'taxi_lat', 'taxi_lng', 'user_id', 'userinit_lat', 'userinit_lng', 'userfinal_lat', 'userfinal_lng', 'taxibase_fare', 'taxikm_fare']


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
        yield element['user_id', 'taxi_id', 'init_distance', 'final_distance']


#DoFn06: Calculating final distance
class AddFinalDistanceDoFn(beam.DoFn):
    def process(self, element):
        element['total_distance'] = element['init_distance'] + element['final_distance']

        yield element

# class CalculateTransactionAmount(beam.DoFn):
#     def process(self, element):
        


'''Dataflow Process'''
def run_pipeline():

    # Input arguments
    parser = argparse.ArgumentParser(description=('Arguments for the Dataflow Streaming Pipeline'))

    parser.add_argument('--output_bigquery', required=True, help='Table where data will be stored in BigQuery. Format: <dataset>.<table>.')
    parser.add_argument('--bigquery_schema_path', required=True, help='BigQuery Schema Path within the repository.')

    args, pipeline_opts = parser.parse_known_args()

    #Load schema from /schema folder 
    with open(args.bigquery_schema_path) as file:
            input_schema = json.load(file)

    schema = bigquery_tools.parse_table_schema_from_json(json.dumps(input_schema))

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
                |"Add User Processing Time" >> beam.ParDo(AddTimestampDoFn())
        )

        taxi_data = (
            p
                |"Read Taxi data from PubSub" >> beam.io.ReadFromPubSub(subscription=f"projects/{project_id}/subscriptions/{input_taxi_subscription}", with_attributes = True)
                |"Parse Taxi JSON messages" >> beam.Map(ParsePubSubMessage)
                |"Add Taxi Processing Time" >> beam.ParDo(AddTimestampDoFn())
        )

        ###Step02: Merge Data from taxi and user topics into one PColl
        # Here we have taxi and user data in the same  table
        data = (user_data, taxi_data) | beam.Flatten()

        ###Step05: Get the closest driver for the user per Window
        (
            data 
                 |"Get location fields." >> beam.ParDo(getLocationsDoFn())
                 |"Call Google maps API to calculate distances between user and taxis" >> beam.ParDo(CalculateInitDistancesDoFn())
                 |"Call Google maps API to calculate distances between user_init_loc and user_final_loc" >> beam.ParDo(CalculateFinalDistancesDoFn())
                 |"Removing locations from data once init and final distances are calculated" >> beam.ParDo(RemoveLocations()) 
                 |"Set fixed window" >> beam.WindowInto(window.FixedWindows(60))
                 |"Get shortest distance between user and taxis" >> MatchShortestDistance()
                 |"Calculate total distance" >> beam.ParDo(AddFinalDistanceDoFn())
                 #|"Calculate transaction amount" >> beam.ParDo(CalculateTransactionAmount())
         )



         ###Step06: Write combined data to BigQuery
        (
            data | "Write to BigQuery" >> beam.io.WriteToBigQuery(
                table = f"{project_id}:{args.output_bigquery}",
                schema = schema,
                create_disposition = beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition = beam.io.BigQueryDisposition.WRITE_APPEND
            )
        )
        

if __name__ == '__main__' : 
    #Add Logs
    logging.getLogger().setLevel(logging.INFO)
    #Run process
    run_pipeline()
