import datetime
from typing import Optional
from app.dto.historic_data import Coordinates, DataSource, TerrorEvent


def create_terror_event(
        title: str,
        content: str,
        date: datetime,
        location: str,
        coordinates: Optional[Coordinates],
        source: DataSource
    ) -> TerrorEvent:
    return TerrorEvent(
        title=title,
        content=content,
        publication_date=date,
        category="historic_terror",
        location=location,
        confidence=1.0,
        source_url=source.value,
        coordinates=coordinates
    )