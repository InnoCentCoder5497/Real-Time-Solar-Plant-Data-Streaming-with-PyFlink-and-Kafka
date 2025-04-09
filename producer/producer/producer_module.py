import time
from kafka import KafkaProducer
import json
import argparse

TOPIC_NAME = 'test-topic'

def stream_plant_data():
    parser = argparse.ArgumentParser(description="Plant data streaming Producer")
    parser.add_argument("--plant_id", required=True, help="The plant id to identify the Plant file to stream from")
    
    args = parser.parse_args()
    print(f'{args}')

def main():
    print('connecting to kafka')
    kafka_broker = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    print('starting msg send')
    i = 0
    try:
        while True:
            msg = {'key1': f'this is key - {i}', 'key2': f'Some value {i}'}
            
            kafka_broker.send(TOPIC_NAME, value=msg)
            print(f'{i} : {msg}')
            time.sleep(10)
            i = i + 1
    except KeyboardInterrupt:
        print('Stopped producer')
        kafka_broker.flush()

if __name__ == '__main__':
    main()
            