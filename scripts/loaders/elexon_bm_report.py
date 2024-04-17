import requests
import pandas as pd
import datetime
from dataclasses import dataclass
from scripts.utils.event_manager import EventManager
from scripts.utils.csv_maker import CSVMaker

BASE_URL = 'https://data.elexon.co.uk/bmrs/api/v1/generation/actual/per-type/wind-and-solar'
SOURCE = 'elexon_bm_report'

KEY_MAP = {
    "psrType": "keys",
    "quantity": 'value',
    "settlementDate": "date",
    "settlementPeriod": "period",
}

REQUIRED_COLUMNS = ['date', 'value', 'keys', "name"]

@dataclass
class BMRSDataType:
    date: str
    value: float
    keys: str
    name: str

def check_date_range(start_date: str, end_date: str) -> None:
    start = datetime.datetime.strptime(start_date, '%Y-%m-%dT%H:%M:%S.%fZ')
    end = datetime.datetime.strptime(end_date, '%Y-%m-%dT%H:%M:%S.%fZ')
    delta = end - start

    if delta.days > 7:
        raise ValueError("Date range should not exceed 7 days.")
    if delta.days < 0:
        raise ValueError("Start date should be before end date.")

def fetch_data(url: str) -> list[dict]:
    response = requests.get(url)
    response.raise_for_status()
    return response.json().get("data", [])

def transform_data(data: list[dict]) -> list[BMRSDataType]:
    if not data:
        return []

    df = pd.DataFrame(data)
    df['name'] = SOURCE
    df = df.rename(columns=KEY_MAP)
    df["keys"] = df["keys"].str.lower()
    df['date'] = pd.to_datetime(df['date']) + pd.to_timedelta((df['period'] - 1) * 30, unit='m')
    df = df.dropna(subset=REQUIRED_COLUMNS)
    
    return df[REQUIRED_COLUMNS].to_dict(orient='records')

def get_elexon_bm_report(start_date: str, end_date: str, event_manager: EventManager) -> None: 
    check_date_range(start_date, end_date)

    url = f'{BASE_URL}?format=json&from={start_date}&to={end_date}'

    data = fetch_data(url)

    transformed_data = transform_data(data)

    event_manager.notify("dataEmit", transformed_data)

def loader_runner():
    event_manager = EventManager()
    csv_maker = CSVMaker(output_folder_path="curvefiles")

    event_manager.subscribe("dataEmit", csv_maker)

    start_date = (datetime.datetime.now() - datetime.timedelta(days=7)).strftime('%Y-%m-%dT00:00:00.000Z')
    end_date = datetime.datetime.now().strftime('%Y-%m-%dT00:00:00.000Z')

    get_elexon_bm_report(start_date, end_date, event_manager)