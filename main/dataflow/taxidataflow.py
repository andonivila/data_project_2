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

# Initial variables
project_id = "data-project-2-376316"
input_taxi_subscription = "taxi_position-sub"
input_user_subscription = "user_position-sub"
output_topic = "surge_pricing"
API_KEY = 'AIzaSyBMazxFGKqM5rDVWyDiFSpESzqjLNgjY4U'

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

    def __init__(self):
        super().__init__()

    def expand(self, pcoll):
        return (pcoll
                | util.group_by_key()
                | util.Map(self._find_closest_match)
                )

    def _find_closest_match(self, group_key, group_values):
        
        # User leads the window
        user_id = group_key
        taxi_id, distances = zip(*group_values)
        shortest_distance = min(distances)
        closest_taxi_index = distances.index(shortest_distance)

        return (user_id, (taxi_id[closest_taxi_index], shortest_distance))


'''DoFn Classes'''

#DoFn01: Add processing timestamp
class AddTimestampDoFn(beam.DoFn):

    #Process function to deal with data
    def process(self, element):
        #Add Processing time field
        element['Processing_Time'] = str(datetime.now())
        yield element

#DoFn02: Get the location fields
class getLocationsDoFn(beam.DoFn):
    def process(self, element):
        yield element['Taxi_id', 'Taxi_lat', 'Taxi_lng', 'user_id', 'Userinit_lat', 'Userinit_lng', 'Userfinal_lat', 'Userfinal_lng']

class calculateDistancesDoFn(beam.DoFn):
    def process(self, element):

        # credentials = Credentials.from_service_account_file("./dataflow/data-project-2-376316-6817462f9a56.json")

        taxi_lat = element['Taxi_lat']
        taxi_long = element['Taxi_lng']
        user_init_lat = element['Userinit_lat']
        user_init_long = element['Userinit_lng']
        #user_final_lat = element['Userfinal_lat']
        #user_final_long = element['Userfinal_lng']

        taxi_position = taxi_lat, taxi_long
        user_intit_position = user_init_lat, user_init_long
        #user_destination = user_final_lat, user_final_long

        # Realiza una solicitud a la API de Google Maps
        gmaps = googlemaps.Client(key=API_KEY) 

        # Accedemos al elemento distance del JSON rebido
        element['init_distance'] = gmaps.distance_matrix(taxi_position, user_intit_position, mode='driving')["rows"][0]["elements"][0]['distance']["value"]

        yield element['init_distance']

'''Dataflow Process'''
def run_pipeline():

    # Input arguments
    parser = argparse.ArgumentParser(description=('Arguments for the Dataflow Streaming Pipeline'))

    parser.add_argument('--hostname', required=True, help='API Hostname provided during the session.')
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
                |"Parse JSON messages" >> beam.Map(ParsePubSubMessage)
                |"Add Processing Time" >> beam.ParDo(AddTimestampDoFn())
        )

        taxi_data = (
            p
                |"Read Taxi data from PubSub" >> beam.io.ReadFromPubSub(subscription=f"projects/{project_id}/subscriptions/{input_taxi_subscription}", with_attributes = True)
                |"Parse JSON messages" >> beam.Map(ParsePubSubMessage)
                |"Add Processing Time" >> beam.ParDo(AddTimestampDoFn())
        )

        ###Step02: Merge Data from taxi and user topics into one PColl
        # Here we have taxi and user data in the same  table
        data = (user_data, taxi_data) | beam.Flatten()

        ###Step04: Write combined data to BigQuery
        (
            data | "Write to BigQuery" >> beam.io.WriteToBigQuery(
                table = f"{project_id}:{args.output_bigquery}",
                schema = schema,
                create_disposition = beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition = beam.io.BigQueryDisposition.WRITE_APPEND
            )
        )

        ###Step05: Get the closest driver for the user per Window

        # (
        #    data 
        #         |"Get location fields." >> beam.ParDo(getLocationsDoFn())
        #         |"Call Googlme maps API to calculate distances between user and taxis" >> beam.ParDo(calculateDistancesDoFn())
        #         |"Set fixed window" >> beam.WindowInto(window.FixedWindows(60))
        #         |"Get shortest distance between user and taxis" >> MatchShortestDistance()
        # )
        



        
    

if __name__ == '__main__' : 
    #Add Logs
    logging.getLogger().setLevel(logging.INFO)
    #Run process
    run_pipeline()
