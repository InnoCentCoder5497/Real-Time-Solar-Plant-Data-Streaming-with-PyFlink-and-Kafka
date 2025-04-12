import time
import json
import argparse
import pickle
import pandas as pd
import os
import threading
from kafka import KafkaProducer

kafka_producer = None

class DataGenerator:
    def __init__(self, plant_id, date_time_col = 'DATE_TIME'):
        self.plant_id = plant_id
        self.date_col = date_time_col
        
        self.generation_data_path = f'../data/Plant_{plant_id}_Generation_Data.csv'
        self.weather_data_path = f'../data/Plant_{plant_id}_Weather_Sensor_Data.csv'
        
        self.checkpoint_file = f'./tmp/checkpoint_plant_{plant_id}.pkl'
        
        try:
            # Solar generator data file
            self.gen_df = pd.read_csv(self.generation_data_path)
            self.gen_df[date_time_col] = pd.to_datetime(self.gen_df[date_time_col], dayfirst=True)
            self.gen_groups = self.gen_df.groupby(date_time_col)
            
            # Weather data file
            self.weather_df = pd.read_csv(self.weather_data_path)
            self.weather_df[date_time_col] = pd.to_datetime(self.weather_df[date_time_col])
            self.weather_groups = self.weather_df.groupby(date_time_col)

            # get ordered data time points
            self.last_checkpoint = min(self.gen_groups.groups.keys())
            
            self.time_delta = pd.Timedelta('15 min')
            
            if os.path.exists(self.checkpoint_file):
                self._load_checkpoint()
            else:
                os.mkdir('./tmp/')

        except Exception as e:
            print(e)
        
    def __repr__(self):
        cls_name = self.__class__.__name__
        return f'{cls_name}(PLANT_ID={self.plant_id}, last_checkpoint={self.last_checkpoint})'
    
    def _save_checkpoint(self):
        with open(self.checkpoint_file, 'wb') as f:
            pickle.dump(self.last_checkpoint, f)
            
    def _load_checkpoint(self):
        with open(self.checkpoint_file, 'rb') as f:
            self.last_checkpoint = pickle.load(f)
            
    def get_timestamp_data(self, dt):
        gen_dt_df = None
        weather_dt_df = None
        if dt in self.gen_groups.groups:
            gen_dt_df = self.gen_groups.get_group(dt)
        
        if dt in self.weather_groups.groups:
            weather_dt_df = self.weather_groups.get_group(dt)
            
        return gen_dt_df, weather_dt_df
    
    def __iter__(self):
        while True:
            dt = self.last_checkpoint
            generator_df, weather_df = self.get_timestamp_data(dt)
            
            self.last_checkpoint = dt + self.time_delta
            self._save_checkpoint()
            yield dt, generator_df, weather_df    
            
            
def send_data_to_kafka(df, plant_id, data_source, mode = 'prod'):
    print(f'{data_source}: Transmitting data for Plant ID {plant_id}')
    
    KAFKA_TOPIC = 'generator-topic'
    if data_source == 'WEATHER':
        KAFKA_TOPIC = 'weather-topic'    
    
    for _, x in df.iterrows():
        data_point = x.to_dict()
        source_key = data_point.pop('SOURCE_KEY')
        plant_key = data_point.pop('PLANT_ID')
        key = {'plant_id': plant_key, 'source_key': source_key}
        data_point['DATE_TIME'] = data_point['DATE_TIME'].strftime('%Y-%m-%d %H:%M:%S')
        
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
    
    if mode != 'test':
        kafka_producer = KafkaProducer(
                            bootstrap_servers='localhost:9092', 
                            key_serializer = lambda x: json.dumps(x).encode('utf-8'),
                            value_serializer=lambda v: json.dumps(v).encode('utf-8')
                        )
    
    print(f'Starting Stream for plant_id = {plant_id}')
    plant_data = DataGenerator(plant_id=plant_id)
    for timestamp, gen_data, weather_data in plant_data:
        print(f'INFO: Data recieved at {timestamp}')
        t1 = threading.Thread(target=send_data_to_kafka, args=(gen_data, plant_id, 'GENERATOR', mode))
        t2 = threading.Thread(target=send_data_to_kafka, args=(weather_data, plant_id, 'WEATHER', mode))
        
        t1.start()
        t2.start()
        
        t1.join()
        t2.join()
        
        print('INFO: Waiting for next data point')
        time.sleep(5)