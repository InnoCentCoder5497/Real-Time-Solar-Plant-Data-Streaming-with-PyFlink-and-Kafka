import time
import json
import argparse
import threading
from kafka import KafkaProducer
from . import DataGenerator

kafka_producer = None           
            
def send_data_to_kafka(df, plant_id, data_source, mode = 'prod'):
    print(f'{data_source}: Transmitting data for Plant ID {plant_id}')
    
    KAFKA_TOPIC = 'generator-topic'
    
    if df is None:
        print('WARN: No data point')
        return 
    
    for _, x in df.iterrows():
        data_point = x.to_dict()
        source_key = data_point.pop('SOURCE_KEY')
        plant_key = data_point.pop('PLANT_ID')
        key = {'plant_id': plant_key, 'source_key': source_key}
        data_point['DATE_TIME'] = data_point['DATE_TIME'].strftime('%Y-%m-%d %H:%M:%S')
        
        for k, v in key.items():
            data_point[k.upper()] = v
        
        if mode == 'test':
            print(f'DRYRUN: sent data for {data_source} Data point Key={key}, value={data_point}')
        else:
            kafka_producer.send(KAFKA_TOPIC, value=data_point, key=key)

def stream_plant_data():
    global kafka_producer
    parser = argparse.ArgumentParser(description="Plant data streaming Producer")
    parser.add_argument("--plant_id", required=True, help="The plant id to identify the Plant file to stream from")
    parser.add_argument("--mode", required=False, default='prod', help="Used to run the producer in test mode (set value to test)")
    
    args = parser.parse_args()

    plant_id = args.plant_id
    mode = args.mode
    
    data_path = f'../data/Plant_{plant_id}_Generation_Data.csv'
    source = 'GENERATOR'
    
    if mode != 'test':
        kafka_producer = KafkaProducer(
                            bootstrap_servers='localhost:9092', 
                            key_serializer = lambda x: json.dumps(x).encode('utf-8'),
                            value_serializer=lambda v: json.dumps(v).encode('utf-8')
                        )
    
    print(f'Starting Stream for plant_id = {plant_id}')
    plant_data = DataGenerator(data_path=data_path, plant_id=plant_id, source=source)
    for timestamp, gen_data in plant_data:
        print(f'INFO: Data recieved at {timestamp}')
        t1 = threading.Thread(target=send_data_to_kafka, args=(gen_data, plant_id, source, mode))
        t1.start()
        t1.join()
        
        print('INFO: Waiting for next data point')
        time.sleep(1)