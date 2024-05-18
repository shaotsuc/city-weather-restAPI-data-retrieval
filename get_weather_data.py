from dotenv import load_dotenv
import os
import requests
import pandas as pd
import json
import csv
import geonamescache
load_dotenv()

## URL & authentication
apik = os.environ.get('apikey')
base_url = 'https://api.weatherapi.com/v1/current.json'

## city list 
city = ['Paris', 'London','New York', 'Tokyo', 'Los Angeles', 'Liverpool', 'Amsterdam', 'Brooklyn', 'Berlin', 'Munich', 'Hamburg', 'Taipei']


## data transformation
i = 0
for c in city:
    if i < len(city):
        url = f"{base_url}?key={apik}&q={city[i]}"
        response = requests.get(url)
        output = json.loads(response.text)
        df = pd.json_normalize(output)
    else:
        break
    i += 1
        
    ## desired data
    weather = {
                'city': df['location.name'][0],
                'country': df['location.country'][0],
                'local_time': df['location.localtime'][0],
                'timezone_name': df['location.tz_id'][0],
                'temperature_c': df['current.temp_c'][0],
                'temperature_f': df['current.temp_f'][0],
                'condition_name': df['current.condition.text'][0],
                'latitude': df['location.lat'][0],
                'longitude': df['location.lon'][0],
                'wind_mph': df['current.wind_mph'][0],
                'wind_kph': df['current.wind_kph'][0],
                'humidity': df['current.humidity'][0],
                'uv': df['current.uv'][0]
                }

    ## get column names
    columns = []
    for l in weather:
        columns.append(l)

    ## load data into csv
    file_exists = os.path.isfile("weather_data.csv")
    with open('weather_data.csv', 'a', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=columns)

        if not file_exists:
            writer.writeheader()
            writer.writerow(weather)
        else: 
            writer.writerow(weather)
