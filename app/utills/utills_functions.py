from datetime import datetime

def split_date_fixed(date_str):
    try:
        date_obj = datetime.strptime(date_str, '%d-%b-%y')
        return date_obj.year, date_obj.month, date_obj.day
    except Exception:
        return None, None, None
