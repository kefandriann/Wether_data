import pandas as pd
import os

folder_path = 'C:/Users/kefuz/OneDrive/Desktop/Wether_data/Historical_data/data/raw'
output_dir = "C:/Users/kefuz/OneDrive/Desktop/Wether_data/Historical_data/data/processed"
os.makedirs(output_dir, exist_ok=True)

csv_files = [f for f in os.listdir(folder_path) if f.endswith('.csv')]

df_list = []

for file in csv_files:
    file_path = os.path.join(folder_path, file)

    try:
        df = pd.read_csv(file_path, on_bad_lines="skip")  # ignore les lignes corrompues
        # Extraire le nom de la ville à partir du nom du fichier
        ville = os.path.splitext(file)[0]  # supprime .csv
        df['ville'] = ville  # ajouter une colonne 'ville'
        df_list.append(df)
        print(f"Ajout des données de {ville}")
    except Exception as e:
        print(f"Erreur avec le fichier {file} : {e}")

meteo_global = pd.concat(df_list, ignore_index=True)

meteo_global = meteo_global.rename(columns={
    'temperature_2m_mean (°C)': 'temperature',
    'cloud_cover_mean (%)': 'clouds',
    'surface_pressure_mean (hPa)': 'pressure',
    'relative_humidity_2m_mean (%)': 'humidity',
    'wind_speed_10m_mean (km/h)': 'wind_speed',
    'wind_direction_10m_dominant (°)': 'wind_degree',
    'visibility_mean (undefined)': 'visibility',
    'time': 'date_extraction'
})

meteo_global = meteo_global.drop(columns=['weather_code (wmo code)'])


meteo_global.to_csv(f"{output_dir}/meteo_global.csv", index=False)