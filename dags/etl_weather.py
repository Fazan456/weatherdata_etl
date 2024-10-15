from airflow import DAG
from airflow.providers.https.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import postgresHook
from airflow.decorators import task
from airflow.utils.dates import days_ago


LATITUDE = '51.5074'
LONGITUDE = '-0.1278'
POSTGRES_CONN_ID='postgres_default'
API_CONN_ID='open_meteo_api'

default_args={
    'owner':'airflow',
    'start_date':days_ago(1)
}


with DAG(
dag_id = 'weather_etl_pipline',
default_args = default_args,
schedule_interval = '@daily',
catchup = False) as dags:
    
    @task
    def extract_weather_data():
        http_hook = HttpHook(http_conn_id = API_CONN_ID, method = 'GET')
        endpoint = f'/v1/forecast?latitude={LATITUDE}&longitude={LONGITUDE}&current_weather=true'

        # response = http_hook.run(endpoint)
        response=http_hook.run(endpoint)

        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Failed to fetch weather data: {response.status_code}")
        
    @task()
    def transform_weather_data(weather_data):
        """Transform the extracted weather data."""
        current_weather = weather_data['current_weather']
        transformed_data = {
            'latitude': LATITUDE,
            'longitude': LONGITUDE,
            'temperature': current_weather['temperature'],
            'windspeed': current_weather['windspeed'],
            'winddirection': current_weather['winddirection'],
            'weathercode': current_weather['weathercode']
        }
        return transformed_data
    
    