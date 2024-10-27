from dotenv import load_dotenv
import os
import requests
import pandas as pd


load_dotenv()

## URL & authentication
apik = os.environ.get('apikey')
BASE_URL = 'https://api.weatherapi.com/v1/current.json'


## city list 
city = []

with open('cities.txt', 'r') as f:
    data = f.read().splitlines()
    for i in data:
        city.append(i)

## data transformation
i = 0
for c in city:
    if i < len(city):
        url = f"{BASE_URL}?key={apik}&q={city[i]}"
        api_output = requests.get(url).json()
        data = pd.json_normalize(api_output)

        ## check if the file exists and if the header is needed.
        file_exists = os.path.isfile("raw_weather_data.csv")
        with open('raw_weather_data.csv', 'a',) as f:
            if not file_exists:
                data.to_csv('raw_weather_data.csv', mode='a', index=False, header=True)
            else:
                data.to_csv('raw_weather_data.csv', mode='a', index=False, header=False)
    else:
        break
    i += 1