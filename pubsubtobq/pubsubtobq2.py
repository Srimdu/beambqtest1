#!/usr/bin/env python
import argparse
import json
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
import apache_beam.transforms.window as window

#Variables needed for this pipeline

bq_schema = "Data_Precipitation:float,Date_Full:date,Date_Month:integer,Date_Week_of:integer,Date_Year:integer,Station_City:string,Station_Code:string,\
    Station_Location:string,Station_State:string,Data_Temperature_Avg_Temp:integer,Data_Temperature_Max_Temp:integer,Data_Temperature_Min_Temp:integer,\
    Data_Wind_Direction:integer,Data_Wind_Speed:float"


#project = bucket = 'qwiklabs-gcp-00-6c0786c00719'
#region = 'us-central1'
#topic = 'testtopic1'

def json_parsing(y):
    y1 = json.loads(y)
    #data["timestamp"] = '2008-12-25T03:30:00' #Hardcoded for now. In future it'll include process event timestamp
    return y1

def convert_types(data):
    #Converts string values to their appropriate type.
    data['Data_Precipitation'] = float(data['Data_Precipitation']) if 'Data_Precipitation' in data else None
    data['Date_Full'] = str(data['Date_Full']) if 'Date_Full' in data else None
    data['Date_Month'] = int(data['Date_Month']) if 'Date_Month' in data else None
    data['Date_Week_of'] = int(data['Date_Week_of']) if 'Date_Week_of' in data else None
    data['Date_Year'] = int(data['Date_Year']) if 'Date_Year' in data else None
    data['Station_City'] = str(data['Station_City']) if 'Station_City' in data else None
    data['Station_Code'] = str(data['Station_Code']) if 'Station_Code' in data else None
    data['Station_Location'] = str(data['Station_Location']) if 'Station_Location' in data else None
    data['Station_State'] = str(data['Station_State']) if 'Station_State' in data else None
    data['Data_Temperature_Avg_Temp'] = int(data['Data_Temperature_Avg_Temp']) if 'Data_Temperature_Avg_Temp' in data else None
    data['Data_Temperature_Max_Temp'] = int(data['Data_Temperature_Max_Temp']) if 'Data_Temperature_Max_Temp' in data else None
    data['Data_Temperature_Min_Temp'] = int(data['Data_Temperature_Min_Temp']) if 'Data_Temperature_Min_Temp' in data else None
    data['Data_Wind_Direction'] = int(data['Data_Wind_Direction']) if 'Data_Wind_Direction' in data else None
    data['Data_Wind_Speed'] = float(data['Data_Wind_Speed']) if 'Data_Wind_Speed' in data else None
    return data


def run(project,bucket,region,topic):
 
    pipeline_options = PipelineOptions(beam_args,
    project='{0}'.format(project),
    job_name='pubsubtobq',
    save_main_session=True,
    staging_location='gs://{0}/staging/'.format(bucket),
    temp_location='gs://{0}/temp/'.format(bucket),
    region='{0}'.format(region),
    runner='DataflowRunner',
    streaming=True)
    
    
    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | "ReadFromPubSub" >> beam.io.gcp.pubsub.ReadFromPubSub(topic='projects/{0}/topics/{1}'.format(project,topic)).with_output_types(bytes)
            | "UTF-8 bytes to string" >> beam.Map(lambda x: x.decode("utf-8"))
            | "MessageParse" >> beam.Map(json_parsing)
            | "Giving window size" >> beam.WindowInto(window.FixedWindows(60, 0))
            | "Typeconversion" >> beam.Map(convert_types)
            | "WriteToBigQuery" >> beam.io.WriteToBigQuery(
            '{0}:test_dataset.weather_stream'.format(project),
            schema=bq_schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
        )


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='PubSub to Bigquery Pipeline')
    parser.add_argument('-p', '--project', help='Unique project ID', required=True)
    parser.add_argument('-b', '--bucket', help='Bucket where Dataflow will be using',required=True)
    parser.add_argument('-r', '--region',help='Region in which to run the Dataflow job',required=True)
    parser.add_argument('-t', '--topic', help='The Pubsub Topic', required=True)

    args, beam_args = parser.parse_known_args()
    
    print('Stream Started')

    run(project=args.project, bucket=args.bucket, region=args.region, topic=args.topic)
