from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator


def stream_data():
    print("hello from kafka stream DAG")
default_args = {
    'owner' : 'airscholar',
    'start_date' : datetime(2025, 11, 24,10,00)
}

def get_data():
    import requests

    res = requests.get("https://randomuser.me/api/")
    res = res.json()
    res = res['results'][0]

    return res

def format_data(res):
    data = {}
    location = res['location']

    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['address'] = (f"{str(location['street']['number'])} { location['street']['name']}, "
                      f"{location['city']} ,{location['state']}, {location['country']}"
                       )
    data['pincode'] = location['postcode']
    data['email'] = res['email']
    data['username'] = res['login']['username']
    data['dob'] = res['dob'] ['date']
    data['registered_date'] = res['registered']['date']
    data['phone'] = res['phone']
    data['picture'] = res['picture'] ['medium']

    return data


def stream_data():
    import json
    import time
    from kafka import KafkaProducer
    import requests

    print("Starting Airflow Kafka streaming task...")

    producer = KafkaProducer(
        bootstrap_servers=['broker:29092'],
        max_block_ms=5000,
    )

    # send 5 messages for testing
    for _ in range(5):
        res = requests.get("https://randomuser.me/api/").json()["results"][0]

        msg = json.dumps(res).encode("utf-8")
        producer.send("user_created", msg)
        producer.flush()

        print("Sent message:", msg[:80])  # print first 80 chars
        time.sleep(1)

    print("Finished sending messages.")

with DAG(
        dag_id="kafka_stream_dag",
        default_args=default_args,
        schedule_interval=None,  # run manually for now
        catchup=False,
) as dag:
    streaming_task = PythonOperator(
        task_id="stream_data_from_api",
        python_callable=stream_data,
    )
# stream_data();


