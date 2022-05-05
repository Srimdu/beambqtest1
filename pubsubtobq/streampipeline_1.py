#!/usr/bin/env python
import argparse
import json
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions


bq_schema = "Data_Precipitation:float,Date_Full:date,Date_Month:integer,Date_Week_of:integer,Date_Year:integer,Station_City:string,Station_Code:string,\
    Station_Location:string,Station_State:string,Data_Temperature_Avg_Temp:integer,Data_Temperature_Max_Temp:integer,Data_Temperature_Min_Temp:integer,\
    Data_Wind_Direction:integer,Data_Wind_Speed:float,process_timestamp:timestamp"

class Parsing(beam.DoFn):

    def process(self, element: bytes, timestamp=beam.DoFn.TimestampParam, window=beam.DoFn.WindowParam):
        input_pc = json.loads(element.decode("utf-8"))
        input_pc["timestamp"] = timestamp.to_rfc3339()
        yield input_pc

def run(project,bucket,region,topic,sub):
    argv = [
        '--project={0}'.format(project),
        '--job_name=pubsubtobq',
        '--save_main_session',
        '--staging_location=gs://{0}/staging/'.format(bucket),
        '--temp_location=gs://{0}/temp/'.format(bucket),
        '--region={0}'.format(region),
        '--runner=DataflowRunner']
    
    pipeline_options = PipelineOptions(argv=argv)
    pipeline_options.view_as(StandardOptions).streaming = True
    
    
    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | "ReadFromPubSub" >> beam.io.gcp.pubsub.ReadFromPubSub(subscription=sub, timestamp_attribute=None)
            | "MessageParse" >> beam.ParDo(Parsing)
            | "WriteToBigQuery" >> beam.io.WriteToBigQuery(
            '{0}:test_dataset.weather_stream'.format(project),
            schema=bq_schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND))
        )


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='PubSub to Bigquery Pipeline')
    parser.add_argument('-p', '--project', help='Unique project ID', required=True)
    parser.add_argument('-b', '--bucket', help='Bucket where Dataflow will be using',
                        required=True)
    parser.add_argument('-r', '--region',
                        help='Region in which to run the Dataflow job',
                        required=True)
    parser.add_argument('-p', '--topic', help='The Pubsub Topic', required=True)
    parser.add_argument('-p', '--sub', help='The Scbscription Name', required=True)

    args = vars(parser.parse_args())

    run(project=args['project'], bucket=args['bucket'], region=args['region'], topic=args['topic'],sub=args['sub'])
