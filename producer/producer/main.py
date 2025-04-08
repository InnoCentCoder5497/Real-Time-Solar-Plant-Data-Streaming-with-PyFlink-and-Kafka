import time
from kafka import KafkaProducer
import json

TOPIC_NAME = 'test-topic'

if __name__ == '__main__':
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
            