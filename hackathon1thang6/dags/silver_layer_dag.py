from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, timezone
import os
import json
import boto3
from botocore.config import Config
from dotenv import load_dotenv
import pandas as pd

load_dotenv()

s3_client = boto3.client(
    's3',
    endpoint_url='http://minio:9000',
    aws_access_key_id=os.getenv('MINIO_ROOT_USER'),
    aws_secret_access_key=os.getenv('MINIO_ROOT_PASSWORD'),
    config=Config(
        signature_version='s3v4',
        s3={'addressing_style': 'path'}
    ),
    region_name='us-east-1'
)


try:
    s3_client.create_bucket(Bucket='air-quality-silver')
except Exception as e:
    print(f"Bucket creation error (can be ignored if bucket exists): {str(e)}")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
}

def process_raw_data():
    try:
        
        timestamp = (datetime.now() - timedelta(hours=1)+timedelta(hours=7)).strftime('%Y/%m/%d/%H')
        response = s3_client.list_objects_v2(Bucket='air-quality-data', Prefix=str(timestamp))
        if 'Contents' not in response:
            print("No data found in the bronze layer")
            print(timestamp)
            return

        # Collect all data
        all_data = []
        for obj in response['Contents']:
            try:
                file_obj = s3_client.get_object(Bucket='air-quality-data', Key=obj['Key'])
                file_content = file_obj['Body'].read().decode('utf-8')
                data = json.loads(file_content)
                all_data.append(data)
            except Exception as e:
                print(f"Error processing file {obj['Key']}: {str(e)}")
                continue

        if not all_data:
            print("No valid data found to process")
            return
        
        
        df = pd.DataFrame(all_data)
        df['date'] = df['date'] + 25200
       
        df['timestamp'] = pd.to_datetime(df['date'], unit='s')
        
       
        df['hour'] = df['timestamp'].dt.hour
        df['day_of_week'] = df['timestamp'].dt.day_name()
        df['month'] = df['timestamp'].dt.month
        df['year'] = df['timestamp'].dt.year
        df['is_weekend'] = df['day_of_week'].isin(['Saturday', 'Sunday'])
        
        def get_aqi_category(aqi):
            if aqi == 1: return 'Good'
            elif aqi == 2: return 'Fair'
            elif aqi == 3: return 'Moderate'
            elif aqi == 4: return 'Poor'
            elif aqi == 5: return 'Very Poor'
            else: return 'Unknown'

        df['aqi_category'] = df['aqi'].apply(get_aqi_category)


        
        
        def get_health_impact(row):
            if row['aqi'] <= 2:
                return 'Low Risk'
            elif row['aqi'] == 3:
                return 'Moderate Risk'
            else:
                return 'High Risk'
        
        df['health_impact'] = df.apply(get_health_impact, axis=1)

        
        numeric_columns = ['pm2_5', 'pm10', 'so2', 'o3', 'no', 'no2', 'co', 'nh3']
        df[numeric_columns] = df[numeric_columns].round(2)

        df['timestamp'] = df['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
        df = df.drop(columns=['date'])
       
        timestamp = (datetime.now() + timedelta(hours = 7)).strftime('%Y%m%d/%H')
        silver_key = f'{int(timestamp.split("/")[-1])-1}/processed_data_{timestamp}.json'
        
        
        processed_data = df.to_dict(orient='records')
        
        
        s3_client.put_object(
            Bucket='air-quality-silver',
            Key=silver_key,
            Body=json.dumps(processed_data, indent=2)
        )
    except Exception as e:
        print(f"Error in silver layer processing: {str(e)}")
        raise

with DAG(
    dag_id='silver_layer_dag',
    default_args=default_args,
    description='Process raw air quality data into silver layer',
    schedule_interval='5 */1 * * *',  # a minute 5 after the hour
    start_date=datetime(2025, 1, 1),
    catchup=False
) as dag:

    process_data_task = PythonOperator(
        task_id='process_raw_data',
        python_callable=process_raw_data
    ) 