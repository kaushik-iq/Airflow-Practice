from airflow.sdk import dag, task, asset
from pendulum import datetime
import os
from assest_13 import fetch_data

#this is to create a asset 
@asset(
    schedule=fetch_data,
    uri="/opt/airflow/logs/data/data_processed.txt",
    name = "process_data"
)

def process_data(self):

    os.makedirs(os.path.dirname(self.uri), exist_ok=True)

    with open(self.uri, 'w') as f:
        f.write(f"Data fetched successfully")

    print(f"Data proccess to {self.uri}")