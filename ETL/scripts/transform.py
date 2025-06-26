import pandas as pd
import os

def transform_to_star() -> str:
    try:

        input_file = "/mnt/c/Users/kefuz/OneDrive/Desktop/Wether_data/ETL/data/processed/meteo_global.csv"
        output_dir = "/mnt/c/Users/kefuz/OneDrive/Desktop/Wether_data/ETL/data/star_schema"
        os.makedirs(output_dir, exist_ok=True)

        meteo_data = pd.read_csv(input_file)

        dim_ville_path = f"{output_dir}/dim_ville.csv"
        dim_date_path = f"{output_dir}/dim_date.csv"

        if os.path.exists(dim_ville_path):
            dim_ville = pd.read_csv(dim_ville_path)
        else:
            dim_ville = pd.DataFrame(columns=['ville_id', 'ville', 'latitude', 'longitude'])

        villes_existantes = set(dim_ville['ville'])
        nouvelles_villes = set(meteo_data['ville']) - villes_existantes

        if nouvelles_villes:
            nouveau_id = dim_ville['ville_id'].max() + 1 if not dim_ville.empty else 1
            nouvelles_lignes = pd.DataFrame({
                'ville_id': range(nouveau_id, nouveau_id + len(nouvelles_villes)),
                'ville': list(nouvelles_villes),
                'latitude': [meteo_data[meteo_data['ville'] == ville]['latitude'].values[0] for ville in nouvelles_villes],
                'longitude': [meteo_data[meteo_data['ville'] == ville]['longitude'].values[0] for ville in nouvelles_villes]
            })
            dim_ville = pd.concat([dim_ville, nouvelles_lignes], ignore_index=True)
            dim_ville.to_csv(dim_ville_path, index=False)

        if os.path.exists(dim_date_path):
            dim_date = pd.read_csv(dim_date_path)
        else:
            dim_date = pd.DataFrame(columns=['date_id', 'date', 'jour', 'mois', 'annee'])

        dates_existantes = set(dim_date['date'])
        nouvelles_dates = set(meteo_data['date_extraction']) - dates_existantes

        if nouvelles_dates:
            nouvelles_dates_parsed = [pd.to_datetime(d, format="%Y-%m-%d") for d in nouvelles_dates]
            nouvelles_lignes_dates = pd.DataFrame({
                'date_id': [d.strftime('%Y%m%d') for d in nouvelles_dates_parsed],
                'date': nouvelles_dates_parsed,
                'jour': [d.day for d in nouvelles_dates_parsed],
                'mois': [d.month for d in nouvelles_dates_parsed],
                'annee': [d.year for d in nouvelles_dates_parsed]
            })
            dim_date = pd.concat([dim_date, nouvelles_lignes_dates], ignore_index=True)
            dim_date.to_csv(dim_date_path, index=False)

        meteo_ville = pd.merge(
            meteo_data,
            dim_ville,
            on=['ville', 'latitude', 'longitude'],
            how='left'
        )

        assert not meteo_ville.empty, "[ERREUR] meteo_ville est vide après le merge !"

        meteo_final = pd.merge(
            meteo_ville,
            dim_date.rename(columns={'date': 'date_extraction'}),
            on='date_extraction',
            how='left'
        )

        meteo_ville['date_extraction'] = pd.to_datetime(meteo_ville['date_extraction'])
        
        fact_meteo = meteo_final[[
            'ville_id', 'date_id',
            'temperature', 'pressure', 'humidity',
            'wind_speed', 'wind_degree', 'clouds', 'visibility',
            'main', 'description'
        ]]

        facts_path = f"{output_dir}/fact_weather.csv"
        fact_meteo.to_csv(facts_path, index=False)
    
        return facts_path
    except Exception as e:
        print(f"[ERREUR TRANSFORM] {str(e)}")
        raise e