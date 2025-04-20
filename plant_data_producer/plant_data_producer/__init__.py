import os
import pickle
import pandas as pd

class DataGenerator:
    def __init__(self, plant_id, data_path, source, date_time_col = 'DATE_TIME'):
        self.plant_id = plant_id
        self.date_col = date_time_col
        
        self.data_path = data_path
        self.last_checkpoint = None
        self.checkpoint_file = f'./tmp/checkpoint_plant_{source}_{plant_id}.pkl'
        
        try:
            # Weather data file
            self.weather_df = pd.read_csv(self.data_path)
            self.weather_df[date_time_col] = pd.to_datetime(self.weather_df[date_time_col])
            self.weather_groups = self.weather_df.groupby(date_time_col)

            # get ordered data time points
            self.last_checkpoint = min(self.weather_groups.groups.keys())
            
            self.time_delta = pd.Timedelta('15 min')
            
            if os.path.exists(self.checkpoint_file):
                self._load_checkpoint()
            else:
                os.mkdir('./tmp/')

        except Exception as e:
            print(e)
        
    def __repr__(self):
        cls_name = self.__class__.__name__
        return f'{cls_name}(Weather Data: PLANT_ID={self.plant_id}, last_checkpoint={self.last_checkpoint})'
    
    def _save_checkpoint(self):
        with open(self.checkpoint_file, 'wb') as f:
            pickle.dump(self.last_checkpoint, f)
            
    def _load_checkpoint(self):
        with open(self.checkpoint_file, 'rb') as f:
            self.last_checkpoint = pickle.load(f)
            
    def get_timestamp_data(self, dt):
        weather_dt_df = None
        
        if dt in self.weather_groups.groups:
            weather_dt_df = self.weather_groups.get_group(dt)
            
        return weather_dt_df
    
    def __iter__(self):
        while True:
            dt = self.last_checkpoint
            weather_df = self.get_timestamp_data(dt)
            self.last_checkpoint = dt + self.time_delta
            self._save_checkpoint()
            yield dt, weather_df    
            
  