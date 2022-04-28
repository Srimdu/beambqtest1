#!/bin/bash

gcloud config set project $(gcloud info --format='value(config.project)')

export PROJECT_ID=$(gcloud info --format='value(config.project)')

export BUCKET=${PROJECT_ID}

git clone https://github.com/Srimdu/Beamiotest

gsutil mb gs://$PROJECT_ID

cd Beamiotest

gsutil cp periodic_table.csv gs://$PROJECT_ID/input/periodic_table.csv

bq mk test_dataset

bq mk test_dataset.table1 AtomicNumber:numeric,Element:string,Symbol:string,AtomicMass:float,NumberofNeutrons:numeric,NumberofProtons:numeric,NumberofElectrons:numeric,Phase:string,Natural:string,Metal:string,Nonmetal:string,Metalloid:string,Type:string,Discoverer:string,Year:numeric

pip install apache-beam[gcp]

python3 beam_check.py  --project $PROJECT_ID --bucket $BUCKET
