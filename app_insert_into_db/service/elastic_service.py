import os
from typing import List
from dotenv import load_dotenv
from app.dto.historic_data import TerrorEvent
from app_insert_into_db.db.elastic_connect import elastic_client

load_dotenv(verbose=True)

def save_events_for_terror(events: List[TerrorEvent]) -> None:
    for event in events:
        try:
            elastic_client.index(
                index=os.environ['ES_INDEX_FOR_TERROR'],
                document=event.to_elastic_doc()
            )
        except Exception as e:
            print(f"Error saving event: {e}")