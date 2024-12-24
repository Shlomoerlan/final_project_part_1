from datetime import datetime
from typing import List
import pandas as pd
from app.dto.historic_data import DataSource, TerrorEvent
from app.service.location_service import get_coordinates
from app.service.terror_event_service import create_terror_event
from app.utills.elastic_utills import _parse_date, _extract_coordinates, _create_content, _clean_text


# def import_historic_data(main_csv_path: str, secondary_csv_path: str) -> None:
#
#     print("Processing main CSV file...")
#     main_events = process_main_csv(main_csv_path)
#     save_events_for_terror(main_events)
#     print(f"Saved {len(main_events)} events from main CSV")
#
#     print("Processing secondary CSV file...")
#     secondary_events = process_secondary_csv(secondary_csv_path)
#     save_events_for_terror(secondary_events)
#     print(f"Saved {len(secondary_events)} events from secondary CSV")


def process_main_csv(filepath: str, limit: int = None) -> List[TerrorEvent]:
    print('process_main_csv')
    df = pd.read_csv(filepath, encoding='iso-8859-1')
    if limit:
        df = df.head(limit)
    events = []
    for _, row in df.iterrows():
        try:
            date = _parse_date(row['iyear'], row['imonth'], row['iday'])
            if not date:
                continue

            coordinates = _extract_coordinates(row)
            location = f"{row['city']}, {row['country_txt']}"

            event = create_terror_event(
                title=f"Terror Attack in {location}",
                content=_create_content(row),
                date=date,
                location=location,
                coordinates=coordinates,
                source=DataSource.MAIN_CSV
            )
            events.append(event)
        except Exception as e:
            print(f"Error processing row: {e}")
            continue
    print('process_main_csv finished')
    return events

def process_secondary_csv(filepath: str, limit: int = None) -> List[TerrorEvent]:
    print('process_secondary_csv')
    df = pd.read_csv(filepath, encoding='iso-8859-1')
    if limit:
        df = df.head(limit)
    events = []

    for _, row in df.iterrows():
        try:
            date = datetime.strptime(row['Date'], '%d-%b-%y')
            location = f"{row['City']}, {row['Country']}"
            coordinates = get_coordinates(row['City'], row['Country'])

            event = create_terror_event(
                title=f"Terror Attack in {location}",
                content=_clean_text(row['Description']),
                date=date,
                location=location,
                coordinates=coordinates,
                source=DataSource.SECONDARY_CSV
            )
            events.append(event)
        except Exception as e:
            print(f"Error processing row from secondary CSV: {e}")
            continue
    print('process_secondary_csv finished')
    return events

