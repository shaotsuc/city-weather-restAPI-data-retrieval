from dotenv import load_dotenv
from requests.auth import HTTPBasicAuth
import os
import requests
import json
load_dotenv()

key_value = os.environ.get('apikey')

country_code = 'ES'
city_list = 'madrid'
params = ({'apikey': key_value, 'q': city_list})

## AccuWeather
url = f"http://dataservice.accuweather.com/locations/v1/{country_code}/search"
response = requests.get(url, params=params)


print(response.text)

# if response.status_code == 200:
#     data = json.loads(response)
#     print(data)
# else: 
#     print(f"Error: {response.status_code}")