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
    import time 
    import logging

    producer = KafkaProducer(boostrap_servers=['broker:29092'], max_block_ms=5000)
    curr_time = time.time()
    
    while True:
        if time.time() > curr_time + 60: #1 minute
            break
        try:
            res = get_data()
            producer.send("transaction_updated", json.dumps(res).encode('utf-8'))
        except Exception as e:
            logging.error(f"An error occured: {e}")
            continue
    

with DAG('user_automation',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_data
    )