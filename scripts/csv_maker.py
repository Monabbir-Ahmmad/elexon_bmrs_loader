from interfaces.subscriber import Subscriber
import pandas as pd

class CSVMaker(Subscriber):
    def update(self, data):
        self.save_to_csv(data)

    def save_to_csv(self, data):
        df = pd.DataFrame(data)
        df.to_csv(f'data/output-{pd.Timestamp.now()}.csv', index=False)
