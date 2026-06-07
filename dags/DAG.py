"""
=================================================
Program ini dibuat untuk melakukan automatisasi 
Extract, Transform, dan Load(ETL)
data Airbnb top cities dari PostgreSQL ke file CSV bersih dan Elasticsearch.
Pipeline memiliki tiga task utama: mengambil data dari PostgreSQL, membersihkan
data, dan mengirim data bersih ke Elasticsearch agar dapat divisualisasikan di Kibana.
=================================================
"""
import datetime as dt  
import re  
from pathlib import Path  

import pandas as pd 
from airflow import DAG  
from airflow.operators.python import PythonOperator  
from elasticsearch import Elasticsearch  
from elasticsearch.helpers import streaming_bulk  
from sqlalchemy import create_engine  

POSTGRES_URL = "postgresql+psycopg2://airflow:airflow@postgres:5432/airflow" # Airflow-to-PostgreSQL connection.
TABLE_NAME = "table_m3"  # Isi nama table di variabel
EXTRACT_PATH = Path("/opt/airflow/dags/extracted.csv")  # Buat memasukan csv sementara
CLEAN_PATH = Path("/opt/airflow/dags/data_clean.csv")  # Buat memasukan csv yang sudah di clean
ELASTICSEARCH_URL = "http://elasticsearch:9200"  # Airflow-to-Elasticsearch connection.
ELASTICSEARCH_INDEX = "airbnb_top_cities_clean"  # Buat memasukan Elasticsearch index name yang digunakan Kibana.


def normalize_column_name(column_name):
    """
    Mengubah nama kolom menjadi format lowercase snake_case.

    Parameters:
        column_name: string - nama kolom asli dari dataset.

    Return:
        normalized: string - nama kolom bersih yang konsisten untuk Python, SQL, dan Elasticsearch.
    """
    normalized = column_name.strip().lower()  # Hilangkan spaces luar dan convert semua huruf ke lowercase.
    normalized = re.sub(r"[^a-z0-9]+", "_", normalized)  # Ganti spaces atau symbols dengan underscores.
    normalized = re.sub(r"_+", "_", normalized)  # Hapus underscores berulang2 kali ke single underscore.
    normalized = normalized.strip("_") 
    return normalized  # Return nama kolom yang sudah di cleaned 


def fetch_from_postgresql():
    """
    Mengambil seluruh data mentah dari PostgreSQL table_m3 dan menyimpannya ke CSV sementara.

    Return:
        None - hasil extract disimpan ke EXTRACT_PATH.
    """
    engine = create_engine(POSTGRES_URL)  # Buat Engine Database buat baca data POSTGRESQL
    query = f"SELECT * FROM {TABLE_NAME}"  # Buat SQL query buat ambil raw data yang masih raw dari tabel table_m3.
    df = pd.read_sql(query, engine)  # Eksekusi SQL query dan load resultnya masuk ke DataFrame.
    df.to_csv(EXTRACT_PATH, index=False)  # Save data yang sudah diekstrak sementara buat nanti di clean.


def clean_airbnb_data():
    """
    Membersihkan data Airbnb sesuai instruksi assignment dan menyimpannya ke CSV final.

    Return:
        None - hasil cleaning disimpan ke CLEAN_PATH.

   """
    
    df = pd.read_csv(EXTRACT_PATH, low_memory=False)
    df = df.drop_duplicates()

    df.columns = [normalize_column_name(columns) for columns in df.columns]

    df['host_name'] = df['host_name'].fillna('Unknown Host')
    df['neighbourhood_group'] = df['neighbourhood_group'].fillna('Not Available')
    df['price'] = df['price'].fillna(0)
    df['reviews_per_month'] = df['reviews_per_month'].fillna(0)
    df['last_review'] = df['last_review'].fillna('1900-01-01')
    df['license'] = df['license'].fillna('No License')
    df['last_review'] = pd.to_datetime(df['last_review'], 
                                       errors='coerce').dt.strftime("%Y-%m-%d")
    df['scrape_date'] = pd.to_datetime(df['scrape_date'], 
                                       errors='coerce').dt.strftime("%Y-%m-%d")
    df['has_price'] = df['price'] > 0
    df['has_license'] = df['license'] != 'No License'
    df['host_segment'] = df['calculated_host_listings_count'].apply(
        lambda count: "Single Listing" if count == 1 else "Commercial / Multi-Listing"
    )
    df['listing_key'] = df['city'].astype(str) + '-' + df['id'].astype(str)
    df.to_csv(CLEAN_PATH, index=False)

def generate_elasticsearch(df):
    """
    Membuat dokumen Elasticsearch dari setiap baris DataFrame.

    Parameters: pandas DataFrame - data bersih yang akan dikirimkan ke Elasticsearch
    """

    for record in df.to_dict(orient='records'):
        yield {
            "_index": ELASTICSEARCH_INDEX,
            "_id": record['listing_key'],
            "_source": record,
        }
    

def post_to_elasticsearch():
    """
    Mengirim data CSV bersih ke Elasticsearch menggunakan bulk helper.

    Return:
        None
    """

    df = pd.read_csv(CLEAN_PATH)
    es_client = Elasticsearch(ELASTICSEARCH_URL)
    actions = generate_elasticsearch(df)
    success_count = 0
    failed_count = 0

    for ok, result in streaming_bulk(es_client, 
                                     actions, 
                                     chunk_size=500, 
                                     raise_on_error=False):
        if ok:
            success_count += 1
        else:
            failed_count += 1
    
    print(f"Indexed rows: {success_count}")
    print(f"Failed rows: {failed_count}")



default_args = {
    "owner": "michael_richard",
    "retries": 1,
    "retry_delay": dt.timedelta(minutes=5)
}

with DAG(
    dag_id="airbnb_pipeline",
    default_args=default_args,
    description="ETL Airbnb top cities data from PostgreSQL to Elasticsearch",
    schedule_interval="10,20,30 9 * * 6",
    start_date=dt.datetime(2024, 11, 1),
    catchup=False,
    tags=['airbnb']
) as dag:
    
    fetch_task = PythonOperator(
        task_id = "fetch_from_postgresql",
        python_callable=fetch_from_postgresql
    )

    clean_task = PythonOperator(
        task_id='data_cleaning',
        python_callable=clean_airbnb_data
    )

    post_task = PythonOperator(
        task_id='post_to_elasticsearch',
        python_callable=post_to_elasticsearch
    )

fetch_task >> clean_task >> post_task