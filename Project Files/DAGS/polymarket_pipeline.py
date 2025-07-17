from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import os
import requests
import pandas as pd
from dotenv import load_dotenv
from py_clob_client.constants import POLYGON
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import OrderArgs
from py_clob_client.order_builder.constants import BUY
from datetime import datetime
import posixpath
import csv
import json
from typing import List
from py_clob_client.clob_types import OrderBookSummary  ### Import the actual class from Polymarket's SDK
import time 
import json 
import psycopg2
from airflow.providers.postgres.operators.postgres import PostgresOperator

### Importing all the necessary functions from the polymarket_functions file created earlier

from polymarket_functions  import (
fetch_polymarket_data,
save_df,
filtering_open_markets,
filter_DF_by_tags,
get_orderbooks_df,
upsert_dataframe_to_postgres,
pull_binance_orderbook
)

### Just a default argument dictionary, inputs can be modified 
default_args = {
    'owner': 'you',
    'start_date': datetime(2025, 5 , 26),
    'schedule_interval' : "@daily",
    'retries': 1,
}


def fetch_market_data():
    ### Fetching the initial polymarket data
    data = fetch_polymarket_data(True)
    save_df(data, 'polymarket_market_data_raw')
    return data  ### Returning the data so we can use airflow functionality to pass data to the next branch of the DAG


def process_data_task(**context):
    ### Processing the data without saving each intermediate function's step
    ti = context['ti'] ### Accessing task instance from airflow
    
    markets_data = ti.xcom_pull(task_ids='fetch_markets_data') ### Getting data from the previous task
    
    ### Next needed steps of the pipeline (does not require saving intermediate DFs for efficiency purposes)
    open_markets = filtering_open_markets(markets_data)
    bitcoin_markets = filter_DF_by_tags(['sports','bitcoin','crypto'], open_markets) 
    ### The tags can be edited, I added tags to filter it for efficiency purposes. 
    
    return bitcoin_markets  ### bitcoin_markets will be passed onto the next task

def save_orderbooks(**context):
    ### Generate and save the Polymarket orderbooks from a particular market DF
    ti = context['ti']
    bitcoin_markets = ti.xcom_pull(task_ids='processing_markets')
    
    orderbooks = get_orderbooks_df(bitcoin_markets)
    save_df(orderbooks, 'orderbooks_data_raw')
    return "Polymarket orderbooks saved successfully"

def fetch_binance_orderbook():
    ### Fetch the initial Polymarket data
    binance_orderbook = pull_binance_orderbook('BTCUSDC') ### Only pulling BTC USDC for simplicity.
    save_df(binance_orderbook, 'binance_orderbook_raw')
    return "Polymarket orderbooks saved successfully"  ### Returning the data so we can use airflow functionality to pass data to the next branch of the DAG

    
def upsert_markets_data():
    ### Upserting the raw Polymarket data into PostgreSQL
    df = pd.read_csv('/opt/airflow/output/polymarket_market_data_raw.csv')
    df = df.astype(str)

    conn = psycopg2.connect(
        database='airflow',
        user='airflow',
        password='airflow',
        host='postgres',
        port='5432'
    )
    cur = conn.cursor()

    table_name = 'polymarket_market_data_raw'
    columns = ', '.join([f'"{col}" TEXT' for col in df.columns])
    ### Creating the table if it doesn't exist (for first-time users)
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS "{table_name}" (
        {columns}
    );
    """
    cur.execute(create_table_sql)
    conn.commit()

    ### Add PRIMARY KEY if it does not exist
    try:
        cur.execute(f"""
            ALTER TABLE "{table_name}"
            ADD CONSTRAINT {table_name}_pkey PRIMARY KEY ("question_id");
        """)
        conn.commit()
    except psycopg2.errors.DuplicateObject:
        conn.rollback()
    except Exception as e:
        conn.rollback()
        print(f"Primary key error: {e}")

    ###Upserting the function so that only new rows are added. repeated rows are updated. and rows that dont appear in the new query are left as is.
    from polymarket_functions import upsert_dataframe_to_postgres
    upsert_dataframe_to_postgres(
        df,
        table_name,
        conn,
        unique_keys=["question_id"]  ### Only updates/replaces matching rows
    )

    conn.close()
    return f"{table_name} upserted."

def upsert_orderbooks_data():
    ### Upserting the raw Polymarket Orderbooks data into PostgreSQL

    df = pd.read_csv('/opt/airflow/output/orderbooks_data_raw.csv')
    df = df.astype(str)

    conn = psycopg2.connect(
        database='airflow',
        user='airflow',
        password='airflow',
        host='postgres',
        port='5432'
    )

    cur = conn.cursor()
    table_name = 'orderbooks_data_raw'
    columns = ', '.join([f'"{col}" TEXT' for col in df.columns])

    ### Creating the table if it doesn't exist (for first-time users)
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS "{table_name}" (
        {columns}
    );
    """
    cur.execute(create_table_sql)
    conn.commit()

    ### Adding a UNIQUE constraint on (token_id, timestamp_unix, side)
    try:
        cur.execute(f"""
            ALTER TABLE "{table_name}"
            ADD CONSTRAINT {table_name}_token_time_side_unique UNIQUE ("token_id", "timestamp_unix", "side");
        """)
        conn.commit()
    except psycopg2.errors.DuplicateObject:
        conn.rollback()  ### Constraint already exists
    except Exception as e:
        conn.rollback()
        print(f"Failed to add UNIQUE constraint: {e}")

    ### Upsert the data
    from polymarket_functions import upsert_dataframe_to_postgres
    upsert_dataframe_to_postgres(df, table_name, conn, unique_keys=["token_id", "timestamp_unix", "side"])

    conn.close()
    return f"{table_name} upserted."


def upsert_binance_orderbook_data():
    ### Upserting the raw Binance Orderbooks data into PostgreSQL

    df = pd.read_csv('/opt/airflow/output/binance_orderbook_raw.csv')
    df = df.astype(str)

    conn = psycopg2.connect(
        database='airflow',
        user='airflow',
        password='airflow',
        host='postgres',
        port='5432'
    )

    cur = conn.cursor()
    table_name = 'binance_orderbook_raw'
    columns = ', '.join([f'"{col}" TEXT' for col in df.columns])

    ### Creating the table if it doesn't exist (for first-time users)
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS "{table_name}" (
        {columns}
    );
    """
    cur.execute(create_table_sql)
    conn.commit()

    ### Adding a UNIQUE constraint on (currency_pair, retrieve_time, side, price)
    try:
        cur.execute(f"""
            ALTER TABLE "{table_name}"
            ADD CONSTRAINT {table_name}_pair_time_side_price_unique_price UNIQUE ("currency_pair", "retrieve_time", "side", "price");
        """)
        conn.commit()
    except psycopg2.errors.DuplicateObject:
        conn.rollback()  ### Constraint already exists
    except Exception as e:
        conn.rollback()
        print(f"Failed to add UNIQUE constraint: {e}")

    ### Perform upsert
    from polymarket_functions import upsert_dataframe_to_postgres
    upsert_dataframe_to_postgres(
        df,
        table_name,
        conn,
        unique_keys=["currency_pair", "retrieve_time", "side", "price"]
    )

    conn.close()
    return f"{table_name} upserted."


with DAG(
    'polymarket_data_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:
    
    fetch_task = PythonOperator(
        task_id='fetch_markets_data',
        python_callable=fetch_market_data
    )
    
    processing_task = PythonOperator(
        task_id='processing_markets',
        python_callable=process_data_task,
        provide_context=True
    )
    
    save_orderbooks_task = PythonOperator(
        task_id='save_orderbooks',
        python_callable=save_orderbooks,
        provide_context=True
    )

    fetch_binance_orderbook_task = PythonOperator(
        task_id='fetch_binance_orderbook',
        python_callable=fetch_binance_orderbook,
        provide_context=True
    )

    upsert_markets_task = PythonOperator(
        task_id='upsert_markets_data',
        python_callable=upsert_markets_data
    )

    upsert_orderbooks_task = PythonOperator(
        task_id='upsert_orderbooks_data',
        python_callable=upsert_orderbooks_data
    )
    
    upsert_binance_orderbook_task = PythonOperator(
    task_id='upsert_binance_orderbook_data',
    python_callable=upsert_binance_orderbook_data
    )


    transform_data_task = PostgresOperator( ###Task to simply re-create the data into the proper data types onto the database
    task_id='transform_data',
    postgres_conn_id='postgres_default',  ### Define in Airflow UI or use Airflow env
    sql='sql/transform_polymarket_data_function.sql',  ### Path to .sql script
    )

    merge_orderbook_data_task = PostgresOperator( ### Task to merge the orderbook data with the market data
    task_id='merge_orderbook_data',
    postgres_conn_id='postgres_default',  ### Define in Airflow UI or use Airflow env
    sql='sql/Merging_orderbooks_with_market_function.sql',  ### Path to .sql script
    )

    create_is_arbitrage_table_task = PostgresOperator( ### Task to merge the orderbook data with the market data
    task_id='create_is_arbitrage_table',
    postgres_conn_id='postgres_default',  ### Define in Airflow UI or use Airflow env
    sql='sql/creating_is_arbitrage_table.sql',  ### Path to .sql script
    )

    create_price_tracker_table_task = PostgresOperator( ### Task to merge the orderbook data with the market data
    task_id='create_price_tracker_table',
    postgres_conn_id='postgres_default',  ### Define in Airflow UI or use Airflow env
    sql='sql/creating_price_tracker_table.sql',  ### Path to .sql script
    )

### Defining the full DAG chain

### Polymarket DAG chain
fetch_task >> processing_task >> save_orderbooks_task

fetch_task >> upsert_markets_task

save_orderbooks_task >> upsert_orderbooks_task

### Binance DAG chain
fetch_binance_orderbook_task >> upsert_binance_orderbook_task

### tranform depends on all 3 previous upserts
[upsert_markets_task, upsert_orderbooks_task, upsert_binance_orderbook_task] >> transform_data_task

### Rest of the DAG chain
transform_data_task >> merge_orderbook_data_task >> create_is_arbitrage_table_task >> create_price_tracker_table_task

