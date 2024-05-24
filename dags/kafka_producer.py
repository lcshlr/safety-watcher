import uuid
import csv
import os
import random
import requests
from math import sqrt
import json
from kafka import KafkaProducer
import logging
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator


def extract_data():
    res = requests.get("https://randomuser.me/api").json()
    res = res['results'][0]
    return res

def get_behaviors(num_of_words):
    with open(os.environ.get("AIRFLOW_HOME")+"/dags/data/behaviors.csv") as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        n = random.sample(range(1,28),num_of_words)
        line = 1
        random_words = []
        for row in csv_reader:
            if line in n:
                random_words.append(row)
            line+=1
        return random_words

'''
To make data more realistic we consider the center of map (50,50) and we consider 
more the behavior detected in the center more it is likely to be suspicious
'''
def get_location_coef(location):
    center = (50,50)
    distance = sqrt(pow((location[0] - center[0]),2) + pow((location[1] - center[1]),2))
    return round(2 - distance/50, 2)

'''
To make data more realistic we consider it is more likely to have a suspicious behavior
depending on the age
'''
def get_age_coef(age):
    if age > 15 and age < 20:
        return 1.2
    if age >= 20 and age < 30:
        return 1.3
    if age > 65:
        return 0.7
    return 1

'''
To make data more realistic we consider it is more likely to have a suspicious behavior
depending on the time. Ex: if during the night or peak-time so coef is higher
'''
def get_hour_coef(hour):
    if hour >= 6 and hour <= 9 or hour >=16 and hour <= 19:
        return 1.2
    if hour > 0 and hour < 6:
        return 1.3
    return 1

def get_timeframe(hour):
    if hour >= 0 and hour < 6:
        return 'night'
    if hour > 5 and hour < 12:
        return 'morning'
    if hour > 11 and hour < 18:
        return 'afternoon'
    return 'evening' 

def format_data(res):
    behaviors = get_behaviors(3)
    behaviors_score = sum(map(lambda x: int(x[1]), behaviors))
    behaviors = list(map(lambda x: x[0], behaviors))
    location = (round(random.uniform(1,100),2), round(random.uniform(1,100),2))
    hour = random.randint(0,23)
    minutes = random.randint(0,59)
    timeframe = get_timeframe(hour)

    data = {}
    data['id'] = str(uuid.uuid4())
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['age'] = res['dob']['age']
    data['behaviors'] = behaviors
    data['location'] = location
    data['time'] = f"{hour}:{minutes}"
    data['timeframe'] = timeframe
    data['phone'] = res['phone']
    data['picture'] = res['picture']['medium']

    location_coef = get_location_coef(location)
    age_coef = get_age_coef(data['age'])
    hour_coef = get_hour_coef(hour)

    dangerosity_score = (behaviors_score * location_coef * hour_coef * age_coef) * 100 / 25 
    data['dangerosity_score'] = int(dangerosity_score)
    print(data)
    return data

def stream_data():
    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
    for i in range(100):
        try:
            res = extract_data()
            res = format_data(res)
            producer.send('users', json.dumps(res).encode('utf8'))
        except Exception as e:
            logging.error(e)
            continue

default_args = {
    'owner': 'lucas',
    'start_date': datetime(2024, 4, 20, 10, 00)
}

with DAG('kafka_producer',
         default_args=default_args,
         schedule_interval='*/5 * * * *',
         catchup=False) as dag:

    start = DummyOperator(
        task_id="start",
    )

    end = DummyOperator(
        task_id="end",
    )

    streaming_task = PythonOperator(
        task_id='generate_fake_data',
        python_callable=stream_data,
    )

    start >> streaming_task >> end
