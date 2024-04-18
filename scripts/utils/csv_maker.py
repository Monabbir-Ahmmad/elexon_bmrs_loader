import os
from os.path import join, dirname
from dotenv import load_dotenv
import pandas as pd
from interfaces.subscriber import Subscriber

load_dotenv(join(dirname(__file__), ".env"))

class CSVMaker(Subscriber):
    def __init__(self, output_file_name):
        self.output_folder_path = os.getenv("BASE_OUTPUT_FOLDER")
        self.output_file_name = output_file_name
        
        self.create_output_folder()

    def create_output_folder(self):
        if not os.path.exists(self.output_folder_path):
            os.makedirs(self.output_folder_path)

    def update(self, data):
        self.save_to_csv(data)

    def save_to_csv(self, data):
        timestamp = pd.Timestamp.now().strftime("%Y-%m-%d %H-%M-%S")
        file_name = f'{self.output_file_name}-{timestamp}.csv'

        file_path = os.path.join(self.output_folder_path, file_name)

        pd.DataFrame(data).to_csv(file_path, index=False)