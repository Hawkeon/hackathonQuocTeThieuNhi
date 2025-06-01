from airflow import DAG 
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import json
import boto3
from botocore.config import Config
from dotenv import load_dotenv
import psycopg2
import pandas as pd
load_dotenv()

conn = psycopg2.connect(
    host='postgres',
    port=os.getenv('POSTGRES_PORT'),
    database=os.getenv('POSTGRES_DB'),
    user=os.getenv('POSTGRES_USER'),
    password=os.getenv('POSTGRES_PASSWORD')
)

s3_client = boto3.client( 
    's3' ,
    endpoint_url = 'http://minio:9000',
    aws_access_key_id = os.getenv('MINIO_ROOT_USER'),
    aws_secret_access_key = os.getenv('MINIO_ROOT_PASSWORD'),
    config = Config(
        signature_version = 's3v4',
        s3 = {'addressing_style': 'path'}
    ),
    region_name = 'asia-southeast1'
)

try: 
    s3_client.create_bucket(Bucket = 'air-quality-final')
except Exception as e:
    print(f"Bucket creation error (can be ignored if bucket exists): {str(e)}")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
}

with DAG( 
    default_args = default_args,
    dag_id = 'final_layer_dag',
    description = 'Final layer DAG',
    schedule_interval = '0 */1 * * *',
    start_date = datetime(2025, 1, 1),
    catchup = False,
) as dag:
    
    def process_data():
        try:
            
            cursor = conn.cursor()
           

            
            all_data = []
            response = s3_client.list_objects_v2(Bucket='air-quality-silver')
            for obj in response['Contents']:
                file_obj = s3_client.get_object(Bucket='air-quality-silver', Key=obj['Key'])
                file_content = file_obj['Body'].read().decode('utf-8')
                data = json.loads(file_content)
                all_data.extend(data)
            
            
            df = pd.DataFrame(all_data)
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            cursor.execute('''
                truncate table air_quality.air_quality_final;
            ''')
            conn.commit()
            for _, row in df.iterrows():
                cursor.execute('''
                    
                    INSERT INTO air_quality.air_quality_final (
                        station_name, timestamp, hour, day_of_week, month, year, is_weekend,
                        pm2_5, pm10, so2, o3, no, no2, co, nh3,
                        aqi, aqi_category, health_impact
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s
                    )
                ''', (
                    row['station_name'], row['timestamp'], row['hour'], row['day_of_week'], 
                    row['month'], row['year'], row['is_weekend'],
                    row['pm2_5'], row['pm10'], row['so2'], row['o3'],
                    row['no'], row['no2'], row['co'], row['nh3'],
                    row['aqi'], row['aqi_category'], row['health_impact']
                ))
            
            conn.commit()
            print("Data successfully inserted into final layer")

           

            cursor.close()
            
        except Exception as e:
            print(f"Error in final layer processing: {str(e)}")
            raise

    task1 = PythonOperator( 
        task_id = 'test_task',
        python_callable = process_data
    )

    task1