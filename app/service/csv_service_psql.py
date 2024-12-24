import pandas as pd
import numpy as np
from app.data.utils_data import COLUMNS_TO_KEEP
from app.utills.utills_functions import split_date_fixed

global_terrorism_path = './data/global_terrorism.csv'
rand_data_path = './data/RAND_data.csv'

def clean_data(data, columns_to_keep, replace_missing=-99):
    for col in columns_to_keep:
        if col not in data.columns:
            data[col] = np.nan

    data = data.replace(replace_missing, np.nan)

    text_columns = ['country_txt', 'region_txt', 'city', 'attacktype1_txt', 'targtype1_txt', 'weaptype1_txt', 'gname', 'summary']
    numeric_columns = ['latitude', 'longitude', 'nkill', 'nwound', 'nkillter', 'nwoundte', 'nperps']

    for col in text_columns:
        if col in data.columns:
            data[col] = data[col].fillna('Unknown')

    for col in numeric_columns:
        if col in data.columns:
            data[col] = data[col].astype(float).where(pd.notna(data[col]), None)

    return data[columns_to_keep]

data = pd.read_csv(global_terrorism_path, encoding='latin1')
new_data = pd.read_csv(rand_data_path, encoding='latin1')

original_cleaned = clean_data(data, COLUMNS_TO_KEEP)

new_data_cleaned = new_data.rename(columns={
    'Date': 'date',
    'City': 'city',
    'Country': 'country_txt',
    'Perpetrator': 'gname',
    'Weapon': 'attacktype1_txt',
    'Injuries': 'nwound',
    'Fatalities': 'nkill',
    'Description': 'summary'
})

required_columns_new = [
    'iyear', 'imonth', 'iday', 'region_txt', 'latitude', 'longitude',
    'targtype1_txt', 'weaptype1_txt',
    'nkillter', 'nwoundte', 'nperps', 'summary'
]
for col in required_columns_new:
    if col not in new_data_cleaned.columns:
        new_data_cleaned[col] = np.nan

new_data_cleaned['iyear'], new_data_cleaned['imonth'], new_data_cleaned['iday'] = zip(
    *new_data_cleaned['date'].map(split_date_fixed)
)
new_data_cleaned = new_data_cleaned.drop(columns=['date'])

new_data_cleaned = clean_data(new_data_cleaned, required_columns_new)

merged_df = pd.concat([original_cleaned, new_data_cleaned], ignore_index=True)

text_columns = ['country_txt', 'region_txt', 'city', 'attacktype1_txt', 'targtype1_txt', 'weaptype1_txt', 'gname', 'summary']
for col in text_columns:
    merged_df[col] = merged_df[col].fillna('Unknown')


