import time
import json
import argparse
import pickle
import pandas as pd
import os
import threading
# from kafka import KafkaProducer

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
            
            
def send_data_to_kafka(df, plant_id, data_source):
    print(f'{data_source}: Transmitting data for Plant ID {plant_id}')
    for _, x in df.iterrows():
        data_point = x.to_dict()
        data_key = data_point.pop('SOURCE_KEY')
        partition_key = data_point.pop('PLANT_ID')
        data_point['DATE_TIME'] = data_point['DATE_TIME'].strftime('%Y-%m-%d %H:%M:%S')
        print(f'{data_source}: Data point Key={data_key}, Parition key={partition_key}, value={json.dumps(data_point)}')

def stream_plant_data():
    parser = argparse.ArgumentParser(description="Plant data streaming Producer")
    parser.add_argument("--plant_id", required=True, help="The plant id to identify the Plant file to stream from")
    
    args = parser.parse_args()

    plant_id = args.plant_id
    print(f'Starting Stream for plant_id = {plant_id}')
    plant_data = DataGenerator(plant_id=plant_id)
    for timestamp, gen_data, weather_data in plant_data:
        
        t1 = threading.Thread(target=send_data_to_kafka, args=(gen_data, plant_id, 'GENERATOR'))
        t2 = threading.Thread(target=send_data_to_kafka, args=(weather_data, plant_id, 'WEATHER'))
        
        t1.start()
        t2.start()
        
        t1.join()
        t2.join()
        
        print('INFO: Waiting for next data point')
        time.sleep(5)