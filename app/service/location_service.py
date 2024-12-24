from typing import Optional
from geopy.geocoders import Nominatim
from app.dto.historic_data import Coordinates

_geocoder = Nominatim(user_agent="terror_analysis", timeout=10)

def get_coordinates(city: str, country: str) -> Optional[Coordinates]:
    try:
        if not all([city, country]):
            return None
        location = _geocoder.geocode(f"{city}, {country}")
        if location:
            return Coordinates(
                lat=location.latitude,
                lon=location.longitude
            )
        return None
    except Exception as e:
        print(f"Error getting coordinates for {city}, {country}: {e}")
        return None