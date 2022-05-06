gcloud config set project $(gcloud info --format='value(config.project)')

export PROJECT_ID=$(gcloud info --format='value(config.project)')

export BUCKET=${PROJECT_ID}

export TOPIC=testtopic1

export REGION=us-central1

pip3 install apache_beam[gcp]

bq mk test_dataset

bq mk test_dataset.weather_stream Data_Precipitation:float,Date_Full:date,Date_Month:integer,Date_Week_of:integer,Date_Year:integer,Station_City:string,Station_Code:string,Station_Location:string,Station_State:string,Data_Temperature_Avg_Temp:integer,Data_Temperature_Max_Temp:integer,Data_Temperature_Min_Temp:integer,Data_Wind_Direction:integer,Data_Wind_Speed:float

gcloud pubsub topics create $TOPIC

gsutil mb gs://$PROJECT_ID

git clone https://github.com/Srimdu/beambqtest1

cd beambqtest1/pubsubtobq/

python3 pubsubtobq2.py  --project $PROJECT_ID --bucket $BUCKET --topic $TOPIC --region $REGION
