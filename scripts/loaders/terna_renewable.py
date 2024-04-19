import os
import requests
import pandas as pd
from dotenv import load_dotenv
from os.path import join, dirname
from dataclasses import dataclass
from scripts.decorators.retry import retry
from datetime import datetime, timedelta
from scripts.utils.csv_maker import CSVMaker
from scripts.utils.event_manager import EventManager

load_dotenv(join(dirname(__file__), ".env"))

CLIENT_ID = os.getenv("TERMA_RENEWABLE_CLIENT_ID")
CLIENT_SECRET = os.getenv("TERMA_RENEWABLE_CLIENT_SECRET")

BASE_URL = "https://api.terna.it/transparency/v1.0/getrenewablegeneration"
ACCESS_TOKEN_URL = "https://api.terna.it/transparency/oauth/accessToken"

SOURCE = "terna_renewable"

REQUIRED_COLUMNS = ["date", "value", "keys", "name"]

KEY_MAP = {
    "Energy_Source": "keys",
    "Renewable_Generation_GWh": "value",
    "Date": "date"
}

SOURCE_MAP = {
    "Hydro": "hydro",
    "Wind": "wind",
    "Photovoltaic": "spv",
}

@dataclass
class RenewableDataType:
    date: str
    value: float
    keys: str
    name: str


def check_date_range(start_date: str, end_date: str) -> None:
    start = datetime.strptime(start_date, "%d/%m/%Y")
    end = datetime.strptime(end_date, "%d/%m/%Y")
    delta = end - start

    if delta.days < 0:
        raise ValueError("Start date should be before end date.")


def get_access_token() -> str:
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
    }
    data = {
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
    }

    response = requests.post(url=ACCESS_TOKEN_URL, headers=headers, data=data)
    response.raise_for_status()
    return response.json().get("access_token")


@retry(retries=3, delay=10, exceptions=(requests.HTTPError,), exception_condition=lambda e: e.response.status_code == 403)
def fetch_data(url: str, access_token: str, date_from: str, date_to: str, types: list[str]) -> list[dict]:
    headers = {
        "Authorization": f"bearer {access_token}",
    }

    params = {"dateFrom": date_from, "dateTo": date_to, "type": types}

    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    return response.json().get("renewableGeneration", [])


def transform_data(data: list[dict]) -> list[RenewableDataType]:
    if not data:
        return []

    df = pd.DataFrame(data)
    df["name"] = SOURCE
    df = df.rename(columns=KEY_MAP)
    df["keys"] = df["keys"].map(lambda x: SOURCE_MAP.get(x))
    df = df.dropna(subset=REQUIRED_COLUMNS)

    return df[REQUIRED_COLUMNS].to_dict(orient="records")


def get_terna_renewable_data(start_date: str, end_date: str, event_manager: EventManager) -> None:
    check_date_range(start_date, end_date)

    access_token = get_access_token()

    data = fetch_data(
        url=BASE_URL,
        access_token=access_token,
        date_from=start_date,
        date_to=end_date,
        types=list(SOURCE_MAP.keys()),
    )

    transformed_data = transform_data(data)

    event_manager.notify("dataEmit", transformed_data)


def loader_runner() -> None:
    event_manager = EventManager()
    csv_maker = CSVMaker(output_file_name=SOURCE)

    event_manager.subscribe(event="dataEmit", listener=csv_maker)

    today = datetime.today()
    first_day_of_month = today.replace(day=1)
    last_day_of_month = today.replace(day=1, month=today.month % 12 + 1) - timedelta(days=1)

    start_date = first_day_of_month.strftime("%d/%m/%Y")
    end_date = last_day_of_month.strftime("%d/%m/%Y")

    get_terna_renewable_data(
        start_date=start_date,
        end_date=end_date,
        event_manager=event_manager,
    )