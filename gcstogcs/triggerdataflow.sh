gcloud config set project $(gcloud info --format='value(config.project)')

export PROJECT_ID=$(gcloud info --format='value(config.project)')

export BUCKET=${PROJECT_ID}

pip3 install apache_beam[gcp]

git clone https://github.com/Srimdu/beambqtest1

gsutil mb gs://$PROJECT_ID

cd beambqtest1

gsutil cp weather.csv gs://$PROJECT_ID/input/weather.csv

cd beambqtest1/gcstogcs

python3 gcstogcs.py  --project $PROJECT_ID --bucket $BUCKET
