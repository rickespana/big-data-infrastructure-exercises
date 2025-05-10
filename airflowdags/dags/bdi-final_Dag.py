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
    "base_url":"https://samples.adsbexchange.com/readsb-hist/",
    "bucket_name":"flights-adsbexchange",
    "num_files":100
}


# Define the DAG
with DAG(
    dag_id="simple_dag",
    default_args= default_args,
    start_date=datetime(2023, 11, 1),
    end_date=datetime(2024, 11, 1),
    schedule_interval='0 0 1 * *',
    catchup=True
) as dag:

    @task
    def get_flight_data():       
        context = get_current_context()
        execution_date = context["execution_date"]              
        year = execution_date.strftime("%Y")
        month = execution_date.strftime("%m")
        day = execution_date.strftime("%d")
        url = f"{default_args['base_url']}/{year}/{month}/{day}/"
        s3_key = f"{year}/{month}/{day}/"
        print("this is the url: ", url)
        html = requests.get(url)
        soup = bs(html.text,'html.parser')
        links = soup.find_all('a')  
        links = links[:default_args['num_files']]
        hook = S3Hook(aws_conn_id='aws_default')
        for file_link in links:
            if file_link.get('href').endswith('.json.gz'):
                file_url = url + file_link.get('href')
                response = requests.get(file_url)
                if response.status_code == 200:
                    hook.load_bytes(
                        Bucket=default_args['bucket_name'],
                        Key=file_link.get('href'),
                        bytes_data=response.content
                    )
        return
        
    @task
    def process(data):
            print(f"Processing: {data}")
            return data.upper()

    data = get_flight_data()
    processed = process(data)