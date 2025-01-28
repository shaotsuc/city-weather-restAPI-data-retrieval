import os, requests, pandas as pd, pandas_gbq as gbq
from datetime import datetime, timedelta
from google.cloud import bigquery
from google.api_core.exceptions import Conflict
from airflow import DAG
from airflow.operators.python import PythonOperator


## Weather Data API Config
apik = os.environ.get('apikey')
BASE_URL = 'https://api.weatherapi.com/v1/current.json'
city = 'London'
URL = f"{BASE_URL}?key={apik}&q={city}"


## BigQuery Configuration
client = bigquery.Client()
project_id = os.environ.get('project_id')
dataset_id = os.environ.get('dataset_id')
dataset_name = 'raw'
table_name = 'raw_weather_data'



def bq_dataset_connection():
    """Build connection to dataset in BiqQuery
    
        1. check if the dataset exists
        2. if not, create.
    """

    try:
        print('Checking if dataset exists')

        client.create_dataset(dataset=dataset_name)
        print(f'Created dataset: {dataset_name}.')

    except Conflict:
        print(f'{dataset_name} dataset already exists.')


## data transformation
def extract_data():
    """Get weather data via Rest API

        1. get daily weather data
        2. clean column name
        3. load in BigQuery
    """

    raw_result = requests.get(URL)
    table_ref = f'{project_id}.{dataset_name}.{table_name}'

    if raw_result.status_code == 200:
        raw_data = pd.json_normalize(raw_result.json())

        raw_data.columns = raw_data.columns.str.replace('.', '_')

        ## loading dataframe into BQ
        try:
            print(f'Loading into {table_ref}')
            gbq.to_gbq(raw_data, table_ref, project_id=project_id, if_exists='append')
            print('Loaded Successfully')

        except:    
            print('There is some error when loading the data')

    else:
        print("Error in API call.")


default_args = {
    'owner': 'shaotsuc',
    'email': 'shaotsu.chen@gmail.com',
    'retries': 3,
    'retry_delay': timedelta(seconds=30)
}


with DAG(
    dag_id = 'data_ingestion',
    schedule = "* * * * *", 
    default_args=default_args,
    start_date=datetime(2025, 1, 26)) as dag:


    ## check if dataset exists first
    check_dataset = PythonOperator(
        task_id='check_dataset_in_bq',
        python_callable=bq_dataset_connection,
    )


    ## ETL execution for weather data
    etl_data = PythonOperator(
        task_id='etl_data_execution',
        python_callable=extract_data,
    )


check_dataset >> etl_data