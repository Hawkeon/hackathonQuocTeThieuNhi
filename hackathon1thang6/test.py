import psycopg2
import os
from dotenv import load_dotenv
load_dotenv()

# Connection parameters
db_params = {
    'host': 'postgres',
    'port': '5432',
    'database': 'postgres',
    'user': 'postgres',
    'password': '12345'
}

try:
    conn = psycopg2.connect(**db_params)
    print("Connected to PostgreSQL successfully!")
    
    # Test the connection with a simple query
    cur = conn.cursor()
    cur.execute('SELECT version();')
    version = cur.fetchone()
    print(f"PostgreSQL version: {version[0]}")
    
    cur.close()
    conn.close()
    print("Connection closed.")
except Exception as e:
    print(f"Error connecting to PostgreSQL: {str(e)}")


