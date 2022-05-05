#!/usr/bin/env python
import argparse
import json
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions

#Variables needed for this pipeline

input_subscription = "projects/PROJECT_ID/subscriptions/SUBSCRIPTION_NAME"
bq_table = "PROJECT_ID:DATASET_NAME.TABLE_NAME"
bq_schema = "Data_Precipitation:float,Date_Full:date,Date_Month:integer,Date_Week_of:integer,Date_Year:integer,Station_City:string,Station_Code:string,\
    Station_Location:string,Station_State:string,Data_Temperature_Avg_Temp:integer,Data_Temperature_Max_Temp:integer,Data_Temperature_Min_Temp:integer,\
    Data_Wind_Direction:integer,Data_Wind_Speed:float,process_timestamp:timestamp"

class CustomParsing(beam.DoFn):

    def process(self, element: bytes, timestamp=beam.DoFn.TimestampParam, window=beam.DoFn.WindowParam):
        input_pc = json.loads(element.decode("utf-8"))
        input_pc["timestamp"] = timestamp.to_rfc3339()
        yield input_pc

def run(project,bucket,region,topic):
    argv = [
        '--project={0}'.format(project),
        '--job_name=pubsubtobq',
        '--save_main_session',
        '--staging_location=gs://{0}/staging/'.format(bucket),
        '--temp_location=gs://{0}/temp/'.format(bucket),
        '--region={0}'.format(region),
        '--runner=DataflowRunner'
    ]
    
    pipeline_options = PipelineOptions(argv=argv)
    pipeline_options.view_as(StandardOptions).streaming = True
    
    
    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | "ReadFromPubSub" >> beam.io.gcp.pubsub.ReadFromPubSub(topic='projects/{0}/topics/{1}'.format(project,topic), timestamp_attribute=None)
            | "MessageParse" >> beam.ParDo(CustomParsing)
            | "WriteToBigQuery" >> beam.io.WriteToBigQuery(
            '{0}:test_dataset.weather_stream'.format(project),
            schema=bq_schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
        )


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='PubSub to Bigquery Pipeline')
    parser.add_argument('-p', '--project', help='Unique project ID', required=True)
    parser.add_argument('-b', '--bucket', help='Bucket where Dataflow will be using',
                        required=True)
    parser.add_argument('-r', '--region',
                        help='Region in which to run the Dataflow job',
                        required=True)
    parser.add_argument('-t', '--topic', help='The Pubsub Topic', required=True)

    args = vars(parser.parse_args())

    run(project=args['project'], bucket=args['bucket'], region=args['region'], topic=args['topic'])
