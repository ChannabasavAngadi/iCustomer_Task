from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import sqlite3
import logging

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 16),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Initialize the DAG
dag = DAG(
    'etl_user_interaction',
    default_args=default_args,
    description='ETL pipeline for user interaction data',
    schedule_interval=timedelta(days=1),  # Run daily
    catchup=False,
)

# Task 1: Data Ingestion
def ingest_data():
    data = {
        'interaction_id': [1, 2, 3, 4, 5],
        'user_id': [101, -1, 103, 104, -1],  # -1 represents missing user_id
        'product_id': [201, 202, -1, 204, 205],  # -1 represents missing product_id
        'action': ['view', 'click', 'purchase', 'view', 'click'],
        'timestamp': ['2024-02-15 10:00:00', '2024-02-15 11:30:00', '2024-02-15 12:45:00', '2024-02-15 14:00:00', '2024-02-15 15:15:00']
    }
    df = pd.DataFrame(data)
    df.to_csv('/tmp/messy_interaction_data.csv', index=False)
    logging.info("Data ingestion completed.")

ingest_task = PythonOperator(
    task_id='ingest_data',
    python_callable=ingest_data,
    dag=dag,
)

# Task 2: Data Cleaning
def clean_data():
    df = pd.read_csv('/tmp/messy_interaction_data.csv')
    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
    df.fillna({'user_id': -1, 'product_id': -1}, inplace=True)
    df.to_csv('/tmp/cleaned_interaction_data.csv', index=False)
    logging.info("Data cleaning completed.")

clean_task = PythonOperator(
    task_id='clean_data',
    python_callable=clean_data,
    dag=dag,
)

# Task 3: Data Transformation
def transform_data():
    df = pd.read_csv('/tmp/cleaned_interaction_data.csv')
    user_counts = df['user_id'].value_counts().to_dict()
    product_counts = df['product_id'].value_counts().to_dict()
    df['user_interaction_count'] = df['user_id'].map(user_counts)
    df['product_interaction_count'] = df['product_id'].map(product_counts)
    df['interaction_count'] = df['user_interaction_count'] + df['product_interaction_count']
    df.to_csv('/tmp/transformed_interaction_data.csv', index=False)
    logging.info("Data transformation completed.")

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
)

# Task 4: Load Data into SQLite
def load_data():
    conn = sqlite3.connect('/tmp/interactions.db')
    df = pd.read_csv('/tmp/transformed_interaction_data.csv')
    df.to_sql('user_interactions', conn, if_exists='replace', index=False)
    conn.close()
    logging.info("Data loaded into SQLite database.")

load_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    dag=dag,
)

# Define DAG dependencies
ingest_task >> clean_task >> transform_task >> load_task
