from google.cloud import storage
from datetime import datetime
from dotenv import load_dotenv
import pandas as pd
from time import time
import os

key_path = os.path.join('infrastructure/credentials/key.json')

class StorageProvider:
    def __init__(self, bucket):
        self.client = storage.Client.from_service_account_json(key_path)
        self.bucket = self.client.get_bucket(bucket)
        pass

    def save_df_to_storage(self, df, algorithm_name, is_time=None):
        file_name = self.get_df_file_name(algorithm_name, is_time)
        self.bucket.blob(file_name).upload_from_string(df.to_csv(), 'text/csv')

    @staticmethod
    def get_df_file_name(algorithm_name, is_time):
        base_name = "cadena/Times_{}_{}_{}.csv" if is_time else "cadena/Results_{}_{}_{}.csv"
        date_format = datetime.today().strftime('%Y_%m_%d_%H_%M_%S')
        tms = str(time())
        name = base_name.format(algorithm_name, date_format, tms)
        return name

