from datetime import datetime
from typing import Optional
import pandas as pd
from app.dto.historic_data import Coordinates
from app.service.location_service import get_coordinates


def _parse_date(year: int, month: Optional[int], day: Optional[int]) -> Optional[datetime]:
    try:
        return datetime(
            year=int(year),
            month=int(month) if pd.notna(month) else 1,
            day=int(day) if pd.notna(day) else 1
        )
    except ValueError:
        return None

def _extract_coordinates(row: pd.Series) -> Optional[Coordinates]:
    if pd.notna(row['latitude']) and pd.notna(row['longitude']):
        return Coordinates(
            lat=float(row['latitude']),
            lon=float(row['longitude'])
        )
    return get_coordinates(row['city'], row['country_txt'])

def _clean_text(text: str) -> str:
    if pd.isna(text):
        return ""
    try:
        return str(text).strip()
    except:
        return ""

def _create_content(row: pd.Series) -> str:
    if pd.notna(row.get('summary')):
        return _clean_text(row['summary'])

    attack_type = _clean_text(row.get('attacktype1_txt', ''))
    target_type = _clean_text(row.get('targtype1_txt', ''))

    if attack_type and target_type:
        return f"{attack_type} attack targeting {target_type}"
    return "Terror incident"


