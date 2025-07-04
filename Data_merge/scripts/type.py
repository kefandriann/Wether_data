import pandas as pd
import os

df = pd.read_csv("C:/Users/kefuz/Desktop/Wether_data/Data_merge/data/processed/meteo_global.csv")

df['temperature'] = df['temperature'].astype(float)
df['pressure'] = df['pressure'].astype(float)
df['humidity'] = df['humidity'].astype(float)
df['wind_speed'] = df['wind_speed'].astype(float)
df['wind_degree'] = df['wind_degree'].astype(float)
df['clouds'] = df['clouds'].astype(float)

df.to_csv("C:/Users/kefuz/Desktop/Wether_data/Data_merge/data/processed/meteo_global.csv", index=False)