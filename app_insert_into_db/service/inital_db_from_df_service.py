from app_insert_into_db.db.database import session_maker
from app_insert_into_db.db.models import Country, Region, City, Location, AttackType, Event, Group, AttackerStatistic, TargetType


def get_unique_values(data, column):
    return data[column].drop_duplicates().reset_index(drop=True)


def insert_unique(session, model, column_name, values):
    for value in values:
        existing = session.query(model).filter(getattr(model, column_name) == value).first()
        if not existing:
            session.add(model(**{column_name: value}))


def insert_countries_regions_cities(session, data):
    insert_unique(session, Country, "country_name", get_unique_values(data, "country_txt"))
    insert_unique(session, Region, "region_name", get_unique_values(data, "region_txt"))
    insert_unique(session, City, "city_name", get_unique_values(data, "city"))


def insert_locations(session, data):
    for _, row in data.iterrows():
        country = session.query(Country).filter_by(country_name=row['country_txt']).first()
        region = session.query(Region).filter_by(region_name=row['region_txt']).first()
        city = session.query(City).filter_by(city_name=row['city']).first()
        location = Location(
            latitude=row['latitude'],
            longitude=row['longitude'],
            country_id=country.country_id if country else None,
            region_id=region.region_id if region else None,
            city_id=city.city_id if city else None
        )
        session.add(location)


def insert_attack_types(session, data):
    insert_unique(session, AttackType, "attacktype_name", get_unique_values(data, "attacktype1_txt"))


def insert_target_types(session, data):
    insert_unique(session, TargetType, "targettype_name", get_unique_values(data, "targtype1_txt"))


def insert_groups(session, data):
    insert_unique(session, Group, "group_name", get_unique_values(data, "gname"))


def insert_events(session, data):
    for _, row in data.iterrows():
        attack_type = session.query(AttackType).filter_by(attacktype_name=row['attacktype1_txt']).first()
        target_type = session.query(TargetType).filter_by(targettype_name=row['targtype1_txt']).first()
        location = session.query(Location).filter_by(
            latitude=row['latitude'], longitude=row['longitude']
        ).first()
        group = session.query(Group).filter_by(group_name=row['gname']).first()
        event = Event(
            iyear=row['iyear'],
            imonth=row['imonth'],
            iday=row['iday'],
            location_id=location.location_id if location else None,
            attacktype_id=attack_type.attacktype_id if attack_type else None,
            targettype_id=target_type.targettype_id if target_type else None,
            group_id=group.group_id if group else None
        )
        session.add(event)


def insert_attacker_statistics(session, data):
    for _, row in data.iterrows():
        event = session.query(Event).filter_by(
            iyear=row['iyear'],
            imonth=row['imonth'],
            iday=row['iday']
        ).first()
        if event:
            attacker_stat = AttackerStatistic(
                event_id=event.event_id,
                n_kill=row.get('nkill', None),
                n_wound=row.get('nwound', None),
                n_per_ps=row.get('nperps', None),
                n_kill_ter=row.get('nkillter', None),
                n_wound_ter=row.get('nwoundte', None)
            )
            session.add(attacker_stat)


def insert_all_data(data):
    with session_maker() as session:
        try:
            insert_countries_regions_cities(session, data)
            insert_locations(session, data)
            insert_attack_types(session, data)
            insert_target_types(session, data)
            insert_groups(session, data)
            insert_events(session, data)
            insert_attacker_statistics(session, data)
            session.commit()
            print("Data inserted successfully!")
        except Exception as e:
            session.rollback()
            print(f"Error occurred: {e}")
        finally:
            session.close()
