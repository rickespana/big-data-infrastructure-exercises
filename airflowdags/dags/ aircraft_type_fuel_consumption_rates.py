#AWS default connection should be defined so task can work.
from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import get_current_context
from datetime import datetime
import boto3
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import requests
from bs4 import BeautifulSoup as bs
default_args ={
    "url":"https://raw.githubusercontent.com/martsec/flight_co2_analysis/refs/heads/main/data/aircraft_type_fuel_consumption_rates.json",
    "bucket_name":"flight-co2-analysis-bdi",

}

# Define the DAG
with DAG(
    dag_id="get_fuel_consumption_rates",
    default_args= default_args,
   
) as dag:

    @task
    def fuel_consumption_rates():       
        context = get_current_context()
        execution_date = context["execution_date"]             
        year = execution_date.strftime("%Y")
        month = execution_date.strftime("%m")
        day = execution_date.strftime("%d")
    
        response = requests.get(default_args['url'])
        
        hook = S3Hook(aws_conn_id='aws_default'
        )
        hook.load_bytes(
            bucket_name=default_args['bucket_name'],
            key="aircraft_type_fuel_consumption_rates.json",
            bytes_data=response.content,
            replace=True
        )
       
        return "success"
        

    data = fuel_consumption_rates()