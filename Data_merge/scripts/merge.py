import pandas as pd
import os

# Merge recent datas with historical data
df1 = pd.read_csv("C:/Users/kefuz/OneDrive/Desktop/Wether_data/ETL/data/processed/meteo_global.csv")
df2 = pd.read_csv("C:/Users/kefuz/OneDrive/Desktop/Wether_data/Historical_data/data/processed/meteo_global.csv")

columns = list(set(df1.columns).intersection(df2.columns))

df1_common = df1[columns]
df2_common = df2[columns]

df_concat = pd.concat([df1_common, df2_common], ignore_index=True)

df_concat = df_concat[['ville', 'date_extraction', 'temperature', 'pressure', 'humidity', 'wind_speed', 'wind_degree', 'clouds', 'visibility']]

df_concat = df_concat.drop(columns=['visibility'])

df_concat.to_csv("C:/Users/kefuz/OneDrive/Desktop/Wether_data/Data_merge/data/processed/meteo_global.csv", index=False)