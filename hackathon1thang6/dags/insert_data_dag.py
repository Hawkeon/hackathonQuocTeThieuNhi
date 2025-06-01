from airflow import DAG 
from airflow.operators.python import PythonOperator 
from datetime import datetime , timedelta , timezone
import os 
import requests
from dotenv import load_dotenv
import boto3
from botocore.config import Config
import json
load_dotenv()

API_KEY = os.getenv('OPENWEATHER_API_KEY')
URL = "https://api.openweathermap.org/data/2.5/air_pollution"

s3_client = boto3.client(
    's3',
    endpoint_url='http://minio:9000', 
    aws_access_key_id='minioadmin',    
    aws_secret_access_key='minioadmin',
    config=Config(
        signature_version='s3v4',
        s3={'addressing_style': 'path'}
    ),
    region_name='us-east-1'
)

try:
    s3_client.create_bucket(Bucket='air-quality-data')
except Exception as e:
    print(f"Bucket creation error (can be ignored if bucket exists): {str(e)}")

stations = {
    'hoan_kiem': {'lat': 21.0285, 'lon': 105.8542},  
    'ba_dinh': {'lat': 21.0333, 'lon': 105.8167},    
    'dong_da': {'lat': 21.0167, 'lon': 105.8333},    
    'hai_ba_trung': {'lat': 21.0167, 'lon': 105.8500},  
    'cau_giay': {'lat': 21.0333, 'lon': 105.8000},      
    'thanh_xuan': {'lat': 20.9833, 'lon': 105.8000}, 
    'hoang_mai': {'lat': 20.9833, 'lon': 105.8500}, 
    'long_bien': {'lat': 21.0333, 'lon': 105.9000}, 
    'tay_ho': {'lat': 21.0833, 'lon': 105.8167},        
    'ha_dong': {'lat': 20.9667, 'lon': 105.7667},   
    'bac_tu_liem': {'lat': 21.0833, 'lon': 105.7500},  
    'nam_tu_liem': {'lat': 21.0167, 'lon': 105.7500},  
    'dong_anh': {'lat': 21.1667, 'lon': 105.8333},  
    'gia_lam': {'lat': 21.0167, 'lon': 105.9333},    
    'soc_son': {'lat': 21.2500, 'lon': 105.8500}, 
    'me_linh': {'lat': 21.1667, 'lon': 105.7167},   
    'thanh_tri': {'lat': 20.9333, 'lon': 105.8500},
    'thuong_tin': {'lat': 20.8333, 'lon': 105.8667},
    'phu_xuyen': {'lat': 20.7333, 'lon': 105.9000},  
    'ung_hoa': {'lat': 20.6500, 'lon': 105.8500},   
    'chuong_my': {'lat': 20.9167, 'lon': 105.7167},  
    'quoc_oai': {'lat': 20.9833, 'lon': 105.6333},   
    'thach_that': {'lat': 21.0333, 'lon': 105.5167}, 
    'phuc_tho': {'lat': 21.1000, 'lon': 105.5667},   
    'dan_phuong': {'lat': 21.0833, 'lon': 105.6667}, 
    'hoai_duc': {'lat': 21.0333, 'lon': 105.7000},   
    'my_duc': {'lat': 20.7333, 'lon': 105.7167},     
    'ung_hoa': {'lat': 20.6500, 'lon': 105.8500},    
    'phu_quoc': {'lat': 20.8333, 'lon': 105.7833},   
    'son_tay': {'lat': 21.1333, 'lon': 105.5000},    
    'ba_vi': {'lat': 21.2000, 'lon': 105.4167},      
}

default_args = { 
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
}

with DAG(
    dag_id='insert_data_dag_v3',
    default_args=default_args,
    description='RAWDATA',
    schedule_interval='*/15 * * * *',
    start_date=datetime(2023, 1, 1),
    catchup=False
) as dag:

    def insert_data(station):
        vietnam_tz = timezone(timedelta(hours=7))
        now_vn = datetime.now(vietnam_tz)
        timestamp = now_vn.strftime('%Y/%m/%d/%H')
        s3_key = f'{timestamp}/{station}-{now_vn.strftime("%Y-%m-%d-%H-%M")}.json'

        params = {
            'lat': stations[station]['lat'],
            'lon': stations[station]['lon'],
            'appid': API_KEY
        }
        response = requests.get(url=URL, params=params)
        response = response.json()
        response = response['list'][0]
        data = {
            'station_name': station,
            'date': response['dt'],
            'aqi': response['main']['aqi'],
            'pm2_5': response['components']['pm2_5'],
            'pm10': response['components']['pm10'],
            'so2': response['components']['so2'],
            'o3': response['components']['o3'],
            'no': response['components']['no'],
            'no2': response['components']['no2'],
            'co': response['components']['co'],
            'nh3': response['components']['nh3'],
        }

        # Upload to S3 with proper JSON serialization
        s3_client.put_object(
            Bucket='air-quality-data',
            Key=s3_key,
            Body=json.dumps(data, indent=2)
        )
        print(f"Data uploaded for {station}: {json.dumps(data, indent=2)}")

    list_of_task = []
    for station in list(stations.keys()):
        task = PythonOperator(
            task_id=f'insert_data_{station}',
            python_callable=insert_data,
            op_args=[station]
        )
        list_of_task.append(task)
       
    

    
    # Set up task dependencies
    
        

