import pandas as pd
import unicodedata

def clean(chaine):
    if isinstance(chaine, str):
        chaine = unicodedata.normalize('NFKD', chaine).encode('ASCII', 'ignore').decode('utf-8')
        chaine = chaine.strip().lower().replace(" ", "_")
        return chaine
    return chaine

# Define file paths and read csv
meteo_path = "C:/Users/kefuz/Desktop/Wether_data/Data_merge/data/processed/meteo_global.csv"
dim_ville_path = "C:/Users/kefuz/Desktop/Wether_data/Data_merge/data/star_schema/dim_ville.csv"
dim_date_path = "C:/Users/kefuz/Desktop/Wether_data/Data_merge/data/star_schema/dim_date.csv"
fact_path = "C:/Users/kefuz/Desktop/Wether_data/Data_merge/data/star_schema/fact_weather.csv"

meteo = pd.read_csv(meteo_path)
dim_ville = pd.read_csv(dim_ville_path)
dim_date = pd.read_csv(dim_date_path)

# Clean city names in both dataframes
meteo['ville'] = meteo['ville'].apply(clean)
dim_ville['ville'] = dim_ville['ville'].apply(clean)

meteo.columns = meteo.columns.str.strip().str.replace(" ", "_")
dim_ville.columns = dim_ville.columns.str.strip().str.replace(" ", "_")
dim_date.columns = dim_date.columns.str.strip().str.replace(" ", "_")

meteo["ville"] = meteo["ville"].str.strip().str.lower()
dim_ville["ville"] = dim_ville["ville"].str.strip().str.lower()

# Merge meteo with dim_ville
meteo = pd.merge(
    meteo,
    dim_ville,
    on="ville",
    how="left",
    validate="many_to_one"
)

# Check for missing cities
missing_cities = meteo[meteo["ville_id"].isna()]
if not missing_cities.empty:
    print("Certaines villes de meteo_global ne sont pas dans dim_ville :")
    print(missing_cities["ville"].unique())

meteo["date"] = pd.to_datetime(meteo["date"])
dim_date["date"] = pd.to_datetime(dim_date["date"])

meteo = pd.merge(
    meteo,
    dim_date.rename(columns={"date": "date"}),
    on="date",
    how="left",
    validate="many_to_one"
)

# Check for missing dates
missing_dates = meteo[meteo["date_id"].isna()]
if not missing_dates.empty:
    print("Certaines dates de meteo_global ne sont pas dans dim_date :")
    print(missing_dates["date"].dt.strftime("%Y-%m-%d").unique())

fact_weather = meteo.dropna(subset=["ville_id", "date_id"]).copy()
fact_weather["ville_id"] = fact_weather["ville_id"].astype(int)
fact_weather["date_id"] = fact_weather["date_id"].astype(int)

# Reorder columns
fact_weather = fact_weather[[
    "ville_id", "date_id", "temperature", "pressure", "humidity",
    "wind_speed", "wind_degree", "clouds"
]]

# Save the fact table to CSV
fact_weather.to_csv(fact_path, index=False)
print(f"Fichier fact_weather.csv cree avec {len(fact_weather)} lignes valides.")
