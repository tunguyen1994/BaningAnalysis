from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args ={
    'owner': 'airflow',
    'start_date': datetime(2024, 2, 26, 5, 00)
}

def get_data():
    import requests

    url = "https://finances.worldbank.org/resource/zucq-nrc3.json"
    response = requests.get(url)
    if response.status_code == 200:
        # Assuming you want to print or save the JSON response
        data = response.json()
        return data
        # Add logic to save data to a file or database as needed
    else:
        print("Failed to fetch data")



def stream_data():
    import json
    from kafka import KafkaProducer
    res = get_data()
    print(json.dump(res, indent=1))

stream_data()