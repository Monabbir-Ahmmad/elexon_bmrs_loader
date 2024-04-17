from scripts.loaders.elexon_bm_report import get_elexon_bm_report
from scripts.event_manager import EventManager
from scripts.csv_maker import CSVMaker
import datetime

def main():
    event_manager = EventManager()
    csv_maker = CSVMaker(output_folder_path="curvefiles")

    event_manager.subscribe("dataEmit", csv_maker)

    start_date = (datetime.datetime.now() - datetime.timedelta(days=7)).strftime('%Y-%m-%dT00:00:00.000Z')
    end_date = datetime.datetime.now().strftime('%Y-%m-%dT00:00:00.000Z')

    get_elexon_bm_report(start_date, end_date, event_manager)


if __name__ == '__main__':
    main()