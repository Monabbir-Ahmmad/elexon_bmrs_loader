import requests
from dataclasses import dataclass
from datetime import datetime, timedelta, time
from bs4 import BeautifulSoup
from scripts.utils.csv_maker import CSVMaker
from scripts.utils.event_manager import EventManager

BASE_URL = "https://ibex.bg/markets/dam/cross-zonal-capacities/"
SOURCE = "ibex_dam_mcr"

CZC_TABLE_MAP = [
    "price,bg,eur/mwh",
    "price,gr,eur/mwh",
    "bg,gr,atc,mw",
    "gr,bg,atc,mw",
    "bg,gr,crossborderflow,mwh/h",
    "gr,bg,crossborderflow,mwh/h",
]

BGRO_CZC_TABLE_MAP = [
    "price,bg,eur/mwh",
    "price,ro,eur/mwh",
    "bg,ro,atc,mw",
    "ro,bg,atc,mw",
    "bg,ro,crossborderflow,mwh/h",
    "ro,bg,crossborderflow,mwh/h",
]

DUPLICATE_KEYS = set(CZC_TABLE_MAP).intersection(BGRO_CZC_TABLE_MAP)

@dataclass
class DataType:
    date: datetime
    value: float
    keys: str
    name: str


def fetch_populated_html(url: str, date: str) -> BeautifulSoup:
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    }
    payload = {"fromDate": date, "but_search": "Search"}

    with requests.Session() as session:
        res = session.post(url, headers=headers, data=payload)
        res.raise_for_status()
        
    return BeautifulSoup(res.text, features="html.parser")


def parse_table_data(soup: BeautifulSoup, table_class: str, table_map: list[str]) -> list[DataType]:
    table = soup.find("table", {"class": table_class})
    rows = table.find_all("tr")
    data = []
    for row in rows[1:]:
        cells = [cell.text for cell in row.find_all("td")]
        date = datetime.strptime(cells[0], "%Y-%m-%d")
        hour = int(cells[1]) - 1
        date_time = datetime.combine(date, time(hour=hour))

        for value, keys in zip(cells[2:], table_map):
            if value:
                data.append(
                    DataType(date=date_time, value=float(value), keys=keys, name=SOURCE)
                )

    return data


def get_ibex_dam_mcr_data(date: str, event_manager: EventManager) -> None:
    html = fetch_populated_html(url=BASE_URL, date=date)

    czc_table_data = parse_table_data(soup=html, table_class="czc-table", table_map=CZC_TABLE_MAP)
    bgro_czc_table_data = parse_table_data(soup=html, table_class="bgro-czc-table", table_map=BGRO_CZC_TABLE_MAP)

    bgro_czc_table_data = [data for data in bgro_czc_table_data if data.keys not in DUPLICATE_KEYS]

    transformed_data = czc_table_data + bgro_czc_table_data
    
    event_manager.notify("dataEmit", transformed_data)


def loader_runner() -> None:
    event_manager = EventManager()
    csv_maker = CSVMaker(output_file_name=SOURCE)
    event_manager.subscribe(event="dataEmit", listener=csv_maker)

    date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    get_ibex_dam_mcr_data(date=date, event_manager=event_manager)