gcloud config set project $(gcloud info --format='value(config.project)')

export PROJECT_ID=$(gcloud info --format='value(config.project)')

export BUCKET=${PROJECT_ID}

pip3 install apache_beam[gcp]

bq mk test_dataset

bq mk test_dataset.weather Data_Precipitation:float,Date_Full:date,Date_Month:integer,Date_Week_of:integer,Date_Year:integer,Station_City:string,Station_Code:string,Station_Location:string,Station_State:string,Data_Temperature_Avg_Temp:integer,Data_Temperature_Max_Temp:integer,Data_Temperature_Min_Temp:integer,Data_Wind_Direction:integer,Data_Wind_Speed:float

git clone https://github.com/Srimdu/beambqtest1

gsutil mb gs://$PROJECT_ID

cd beambqtest1

gsutil cp weather.csv gs://$PROJECT_ID/input/weather.csv

cd gcstobigquery

python3 gcstobq.py  --project $PROJECT_ID --bucket $BUCKET
