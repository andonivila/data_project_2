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
user_bq_output = "dataproject2376316.user_data"
taxi_bq_output = "dataproject2376316.taxi_data"
taxi_bq_schema_path = "" #Completar
user_bq_schema_path = "" #Completar


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



'''DoFn Classes'''

#DoFn01: Add processing timestamp
class AddTimestampDoFn(beam.DoFn):

    #Process function to deal with data
    def process(self, element):
        #Add Processing time field
        element['Processing_Time'] = str(datetime.now())
        yield element


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

        ###Step01: Read user and taxi data from PUB/SUB and add Timestamp
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

        ###Step02: Write raw data to BigQuery for analytics

        # User data to BigQuery
        (
            user_data | "Write to BigQuery" >> beam.io.WriteToBigQuery(
            table = f"{project_id}:{user_bq_output}",
            schema = schema,
            create_disposition = beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition = beam.io.BigQueryDisposition.WRITE_APPEND
            )
        )

        # Taxi data to BigQuery
        (
            taxi_data | "Write to BigQuery" >> beam.io.WriteToBigQuery(
            table = f"{project_id}:{taxi_bq_output}",
            schema = schema,
            create_disposition = beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition = beam.io.BigQueryDisposition.WRITE_APPEND
            )
        )

        ###Step03: Merge Data from taxi and user topics
        
        
    








run_pipeline()
