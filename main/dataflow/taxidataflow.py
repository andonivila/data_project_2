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
    return ('DP2',row)

def ClaculateDistances(element):

    from googlemaps import Client

    key, data = element
    logging.info(f"This is my raw data: {data}")

    gmaps = Client(key=clv_gm)

    #Calculating distance between users and taxis
    user_init_position = (data["users"][0]["userinit_lat"], data["users"][0]["userinit_lng"])
    taxi_position = (data["taxis"][0]["taxi_lat"], data["taxis"][0]["taxi_lng"])
    user_final_position = (data["users"][0]["userfinal_lat"], data["users"][0]["userfinal_lng"])

    distance_matrix_1 = gmaps.distance_matrix(user_init_position, taxi_position, mode='driving')
    distance_matrix_2 = gmaps.distance_matrix(user_init_position, user_final_position, mode='driving')


    init_distance = distance_matrix_1['rows'][0]['elements'][0]['distance']['value']
    final_distance = distance_matrix_2['rows'][0]['elements'][0]['distance']['value']

    bq_element = {
        'user_id': data["users"][0]["user_id"],
        'taxi_id': data["taxis"][0]["taxi_id"],
        'userinit_lat' : data["users"][0]["userinit_lat"],
        'userinit_lng' : data["users"][0]["userinit_lng"],
        'taxi_lat' : data["taxis"][0]["taxi_lat"],
        'taxi_lng' : data["taxis"][0]["taxi_lng"],
        'init_distance': init_distance,
        'userfinal_lat' : data["users"][0]["userfinal_lat"],
        'userfinal_lng' :  data["users"][0]["userfinal_lng"],
        'final_distance' : final_distance
    }

    return bq_element


#DoFn01: Add processing timestamp
class AddTimestampDoFn(beam.DoFn):

    #Process function to deal with data
    def process(self, element):
        from datetime import datetime

        #Add Processing time field
        element['processing_time'] = str(datetime.now())
        yield element


# #DoFn06: Calculating final distance
# class AddFinalDistanceDoFn(beam.DoFn):
#     def process(self, element):
#         element['total_distance'] = element['init_distance'] + element['final_distance']

#         yield element

# class CalculateTransactionAmount(beam.DoFn):
#     def process(self, element):


'''PTransform Classes'''

class BussinessLogic(beam.PTransform):
    def expand(self, pcoll):
        match = (pcoll
            |"Calculate distances" >> beam.Map(ClaculateDistances)
            |"Key by user_id" >> beam.Map(lambda x: (x['user_id'], x))
            |"Group by user_id" >> beam.GroupByKey()
            |"Find shortest distance" >> beam.Map(lambda x: {
                'user_id': x[0],
                #Aqui podemos ir sacando los campos que queramos de la PColl inicial
                'taxi_id': min(x[1], key=lambda y: y['init_distance'])['taxi_id'],
                'init_distance': min(x[1], key=lambda y: y['init_distance'])['init_distance'],
                'final_distance' : min(x[1], key=lambda y: y['init_distance'])['final_distance']
            }
            )
        )

        return match

        
    
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
                |"Set fixed window_Users" >>  beam.WindowInto(window.FixedWindows(60))
        )

        taxi_data = (
            p
                |"Read Taxi data from PubSub" >> beam.io.ReadFromPubSub(subscription=f"projects/{project_id}/subscriptions/{input_taxi_subscription}", with_attributes = True)
                |"Parse Taxi JSON messages" >> beam.Map(ParsePubSubMessage)
                |"Set fixed window_Taxis" >>  beam.WindowInto(window.FixedWindows(60))
                
        )

        ###Step02: Merge Data from taxi and user topics into one PColl
        # Here we have taxi and user data in the same  table

        data = (({
            "taxis": taxi_data, 
            "users": user_data
            }) | beam.CoGroupByKey()
            |"Business Logic" >> BussinessLogic()
        )

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


    #     class MatchShortestDistance(beam.PTransform):
    # def expand(self, pcoll):
    #     match = (pcoll
    #             |"Key by user_id" >> beam.Map(lambda x: (x['user_id'], x))
    #             |"Group by user_id" >> beam.GroupByKey()
    #             |"Find shortest distance" >> beam.Map(lambda x: {
    #                 'user_id': x[0],
    #                 #Aqui podemos ir sacando los campos que queramos de la PColl inicial
    #                 'taxi_id': min(x[1], key=lambda y: y['init_distance'])['taxi_id'],
    #                 'calculate shortest_distance': min(x[1], key=lambda y: y['init_distance'])['init_distance'],
    #                 'calculate final distance': min(x[1], key=lambda y: y['init_distance'])['final_distance']
    #             })
    #         )

    #     return match
