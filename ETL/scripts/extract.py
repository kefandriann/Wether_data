import os
import requests
import pandas as pd
from datetime import datetime
import logging


def extract_meteo(city: str, api_key: str, date: str) -> bool:
    try:
        url = "https://api.openweathermap.org/data/2.5/weather"
        params = {
            'q': city,
            'appid': api_key,
            'units': 'metric',
        }
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()

        weather_data = {
            'ville': city,
            'date_extraction': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'latitude': data['coord']['lat'],
            'longitude': data['coord']['lon'],
            'temperature': data['main']['temp'],
            'pressure': data['main']['pressure'],
            'humidity': data['main']['humidity'],
            'wind_speed': data['wind']['speed'],
            'wind_degree': data['wind']['deg'],
            'clouds': data['clouds']['all'],
            'visibility': data.get('visibility', None),
            'main': data['weather'][0]['main'],
            'description': data['weather'][0]['description'],
        }

        os.makedirs(f"/mnt/c/Users/kefuz/OneDrive/Desktop/Wether_data/ETL/data/raw/{date}", exist_ok=True)

        pd.DataFrame([weather_data]).to_csv(
            f"/mnt/c/Users/kefuz/OneDrive/Desktop/Wether_data/ETL/data/raw/{date}/meteo_{city}.csv", 
            index=False
        )

        return True
        
    except requests.exceptions.RequestException as e:
        logging.error(f"Erreur réseau/API pour {city}: {str(e)}")
    except KeyError as e:
        logging.error(f"Champ manquant dans la réponse pour {city}: {str(e)}")
    except Exception as e:
        logging.error(f"Erreur inattendue pour {city}: {str(e)}")
        
    return False