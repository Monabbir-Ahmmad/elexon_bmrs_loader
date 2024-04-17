from interfaces.subscriber import Subscriber
import pandas as pd

class CSVMaker(Subscriber):
    def __init__(self, output_folder_path):
        self.output_folder_path = output_folder_path

    def update(self, data):
        self.save_to_csv(data)

    def save_to_csv(self, data):
        df = pd.DataFrame(data)
        df.to_csv(f'{self.output_folder_path}/output-{pd.Timestamp.now()}.csv', index=False)
