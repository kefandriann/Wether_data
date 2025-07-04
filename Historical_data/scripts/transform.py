import pandas as pd
import os

# Define the folder containing the CSV files and the output directory
folder_path = 'C:/Users/kefuz/OneDrive/Desktop/Wether_data/Historical_data/data/raw'
output_dir = "C:/Users/kefuz/OneDrive/Desktop/Wether_data/Historical_data/data/processed"
os.makedirs(output_dir, exist_ok=True)

csv_files = [f for f in os.listdir(folder_path) if f.endswith('.csv')]

df_list = []

for file in csv_files:
    file_path = os.path.join(folder_path, file)

    try:
        df = pd.read_csv(file_path, on_bad_lines="skip")
        # Extract the city name from the filename
        ville = os.path.splitext(file)[0]  # removes the file extension .csv
        df['ville'] = ville  # add the city name as a new column
        df_list.append(df)
    except Exception as e:
        print(f"Error on the file {file} : {e}")

meteo_global = pd.concat(df_list, ignore_index=True)

# Rename columns to remove special characters and spaces
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