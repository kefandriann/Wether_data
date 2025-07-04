import pandas as pd

df = pd.read_csv("C:/Users/kefuz/Desktop/Wether_data/Data_merge/data/processed/meteo_global.csv", sep=';')

df_rain = df[["humidity", "clouds"]].copy()

df_rain['humidity'] = df_rain['humidity'].str.replace(',', '.').astype(float)
df_rain['clouds'] = df_rain['clouds'].str.replace(',', '.').astype(float)

df_rain["clouds"] = pd.to_numeric(df_rain["clouds"], errors="coerce")
df_rain["humidity"] = pd.to_numeric(df_rain["humidity"], errors="coerce")

df["rainy"] = ((df_rain["clouds"] >= 75) & (df_rain["humidity"] >= 80)).astype(int)

df.to_csv("C:/Users/kefuz/Desktop/Wether_data/Data_merge/data/processed/meteo_global.csv", index=False, sep=';', decimal=',')
