import apache_beam as beam
import argparse
from apache_beam.options.pipeline_options import PipelineOptions


def rm_quotes(data):
    """Function used to remove quotes in the data"""
    data[0] = data[0].replace('"','')
    data[-1] = data[-1].replace('"','')
    return data
    
def convert_types(data):
    """Converts string values to their appropriate type."""
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
    
def run(project, bucket):
    argv = [
        '--project={0}'.format(project),
        '--job_name=examplejob',
        '--save_main_session',
        '--staging_location=gs://{0}/staging/'.format(bucket),
        '--temp_location=gs://{0}/temp/'.format(bucket),
        '--region=us-central1',
        '--runner=DataflowRunner'
    ]
    
    table_schema = 'Data_Precipitation:float,Date_Full:date,Date_Month:integer,Date_Week_of:integer,Date_Year:integer,Station_City:string,Station_Code:string,\
    Station_Location:string,Station_State:string,Data_Temperature_Avg_Temp:integer,Data_Temperature_Max_Temp:integer,Data_Temperature_Min_Temp:integer,\
    Data_Wind_Direction:integer,Data_Wind_Speed:float'
        
    file_name1 = 'gs://{}/input/weather.csv'.format(bucket)
    
    file_out1 = 'gs://{}/out'.format(bucket)
    
    with beam.Pipeline(argv=argv) as p:

        (p | 'ReadData' >> beam.io.ReadFromText(file_name1, skip_header_lines =1)
        | 'SplitData' >> beam.Map(lambda x: x.split('","'))
        | 'RemovingQuotes' >> beam.Map(rm_quotes)
        | 'FormatToDict' >> beam.Map(lambda x: {"Data_Precipitation": x[0], "Date_Full": x[1], "Date_Month": x[2], "Date_Week_of": x[3], "Date_Year": x[4], "Station_City": x[5], "Station_Code": x[6], "Station_Location": x[7],
        "Station_State": x[8], "Data_Temperature_Avg_Temp": x[9], "Data_Temperature_Max_Temp": x[10], "Data_Temperature_Min_Temp": x[11], "Data_Wind_Direction": x[12], "Data_Wind_Speed": x[13]}) 
        | 'ChangeDataType' >> beam.Map(convert_types)
        | 'WriteToGCS' >> WriteToJson('file_out1', file_name_suffix='.json',num_shards=1))
    

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Run simple io pipeline on the cloud')
    parser.add_argument('-p', '--project', help='Unique project ID', required=True)
    parser.add_argument('-b', '--bucket', help='Bucket where input file exists', required=True)

    args = vars(parser.parse_args())

    print('Dataflow Job has been started')

    run(project=args['project'], bucket=args['bucket'])
    
    print('Dataflow Job has been completed')
