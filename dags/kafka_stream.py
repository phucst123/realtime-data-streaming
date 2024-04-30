
import uuid
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

random_user_api = 'https://randomuser.me/api/'

default_args = {
    'owner': 'me',
    'start_date': datetime(2023, 4, 30, 8, 00)
}

def get_data():
    import json
    import requests

    response = requests.get(random_user_api).json()
    result = response['results'][0]
    print(json.dumps(result, indent=4))

    return result

def format_data(res):
    data = {}
    data['id'] = str(uuid.uuid4())
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    location = res['location']
    data['address'] = f"{str(location['street']['number'])} {location['street']['name']}, {location['city']}, {location['state']}, {location['country']}"
    data['post_code'] = location['postcode']
    data['email'] = res['email']
    data['username'] = res['login']['username']
    data['dob'] = res['dob']['date']
    data['registered_date'] = res['registered']['date']
    data['phone'] = res['phone']
    data['picture'] = res['picture']['medium']
    return data

def stream_data():
    import json
    import time
    from kafka import KafkaProducer
    import logging

    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)

    current_time = time.time()

    while True:
        if time.time() > current_time + 60: # 1 minute
            break
        try:
            raw_data = get_data()
            data = format_data(raw_data)

            producer.send('users_created', json.dumps(data).encode('utf-8'))

        except Exception as e:
            logging.error(f'An error occured: {e}')
            continue

        

with DAG(
        dag_id='user_automation', 
        default_args=default_args,
        schedule_interval='@daily',
        catchup=False
    ) as dag:
    
    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_data
    )
