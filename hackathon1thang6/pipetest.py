import requests 
import json 
import os 
from dotenv import load_dotenv
import boto3 
from botocore.config import Config
load_dotenv()

API_KEY = os.getenv('OPENWEATHER_API_KEY')
URL = "https://api.openweathermap.org/data/2.5/air_pollution"

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
    'ha_dong': {'lat': 20.9667, 'lon': 105.7667},    # Ha Dong District
    'bac_tu_liem': {'lat': 21.0833, 'lon': 105.7500},  # Bac Tu Liem District
    'nam_tu_liem': {'lat': 21.0167, 'lon': 105.7500},  # Nam Tu Liem District
    'dong_anh': {'lat': 21.1667, 'lon': 105.8333},   # Dong Anh District
    'gia_lam': {'lat': 21.0167, 'lon': 105.9333},    # Gia Lam District
    'soc_son': {'lat': 21.2500, 'lon': 105.8500},    # Soc Son District
    'me_linh': {'lat': 21.1667, 'lon': 105.7167},    # Me Linh District
    'thanh_tri': {'lat': 20.9333, 'lon': 105.8500},  # Thanh Tri District
    'thuong_tin': {'lat': 20.8333, 'lon': 105.8667}, # Thuong Tin District
    'phu_xuyen': {'lat': 20.7333, 'lon': 105.9000},  # Phu Xuyen District
    'ung_hoa': {'lat': 20.6500, 'lon': 105.8500},    # Ung Hoa District
    'chuong_my': {'lat': 20.9167, 'lon': 105.7167},  # Chuong My District
    'quoc_oai': {'lat': 20.9833, 'lon': 105.6333},   # Quoc Oai District
    'thach_that': {'lat': 21.0333, 'lon': 105.5167}, # Thach That District
    'phuc_tho': {'lat': 21.1000, 'lon': 105.5667},   # Phuc Tho District
    'dan_phuong': {'lat': 21.0833, 'lon': 105.6667}, # Dan Phuong District
    'hoai_duc': {'lat': 21.0333, 'lon': 105.7000},   # Hoai Duc District
    'my_duc': {'lat': 20.7333, 'lon': 105.7167},     # My Duc District
    'ung_hoa': {'lat': 20.6500, 'lon': 105.8500},    # Ung Hoa District
    'phu_quoc': {'lat': 20.8333, 'lon': 105.7833},   # Phu Quoc District
    'son_tay': {'lat': 21.1333, 'lon': 105.5000},    # Son Tay District
    'ba_vi': {'lat': 21.2000, 'lon': 105.4167},      # Ba Vi District
}



def insert_data(station) :
    params = { 
        'lat' : stations[station]['lat'],
        'lon' : stations[station]['lon'] , 
        'appid' : API_KEY

    }
    response = requests.get(url= URL , params =  params)
    response = response.json() 
    response = response['list'][0]
    data = { 
        'station_name' : station,
        'aqi' : response['main']['aqi'],
        'pm2_5' : response['components']['pm2_5'] , 
        'pm10' : response['components']['pm10'] , 
        'so2' : response['components']['so2'] , 
        'o3' : response['components']['o3'] ,
        'no' : response['components']['no'] ,
        'no2' : response['components']['no2'] , 
        'co' : response['components']['co'] ,
        'nh3' : response['components']['nh3'] , 
    }
    print(data)
# for station in list(stations.keys()) : 
#     insert_data(station)




s3_client = boto3.client('s3' , 
                         endpoint_url = 'http://localhost:9000' , 
                         aws_access_key_id = os.getenv('MINIO_ROOT_USER') , 
                         aws_secret_access_key = os.getenv('MINIO_ROOT_PASSWORD') , 
                         config = Config(signature_version = 's3v4' , region_name = 'ap-southeast-1' , retries = { 'max_attempts' : 10 , 'mode' : 'standard' }))

try : 
    s3_client.create_bucket(Bucket = 'project-airflow-bucket')
except Exception as e : 
    print(e)














