import pandas as pd
import os

# Concat dim_date
df = pd.read_csv("C:/Users/kefuz/Desktop/Wether_data/Data_merge/data/star_schema/dim_date.csv")

meteo_global = pd.read_csv("C:/Users/kefuz/Desktop/Wether_data/Data_merge/data/processed/meteo_global.csv")
meteo_global['date'] = pd.to_datetime(meteo_global['date'])

# Create the dim_date DataFrame
dim_date = pd.DataFrame()
dim_date['date'] = meteo_global['date']
dim_date['date_id'] = meteo_global['date'].dt.strftime('%Y%m%d')
dim_date['jour'] = meteo_global['date'].dt.day
dim_date['mois'] = meteo_global['date'].dt.month
dim_date['annee'] = meteo_global['date'].dt.year

dim_date = dim_date.drop_duplicates()

dim_date = dim_date[['date_id', 'date', 'jour', 'mois', 'annee']]

dim_date['date'] = pd.to_datetime(dim_date['date'])

# Save the transformed dim_date DataFrame to a CSV file
dim_date.to_csv("C:/Users/kefuz/Desktop/Wether_data/Data_merge/data/star_schema/dim_date.csv", index=False)