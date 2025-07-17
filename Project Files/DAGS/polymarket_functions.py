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
from py_clob_client.clob_types import OrderBookSummary  # Import the actual class from Polymarket's SDK
import time 
import json
import ast
import re
import psycopg2
from psycopg2.extras import execute_values
import logging
from binance.lib.utils import config_logging
from binance.spot import Spot as Client


#### Loading the env file to retrieve the polymarket and binance passwords
load_dotenv()
pk = os.getenv("POLYMARKET_PRIVATE_KEY") ###Private key
public_key = os.getenv("POLYMARKET_PUBLIC_KEY") ### public key
host = "https://clob.polymarket.com"
chain_id = 137
OUTPUT_DIR = '/opt/airflow/output'

#### Using the basic client info to derive FURTHER information on my API key
client = ClobClient(
  host,               # the polymarket host
  key=pk,            # your private key exported from magic/polymarket
  chain_id=chain_id,  # 137
  # creds=creds,        # your api creds (you can leave them empty if you haven't generated/derived them yet)
  signature_type=1,   # this type of config requires this signature type
  funder=  os.getenv("POLYMARKET_FUND_ADDRESS")) # This is your Polymarket public address (where you send the USDC)

api_key=os.getenv("BINANCE_API_KEY")
api_secret=os.getenv("BINANCE_SECRET_KEY")
binance_client = Client(api_key, api_secret)

api_key_data = client.derive_api_key()

#### Using the API Key Data to get further creds - will be able to run more functions

### Will refer to this Client for the rest of the project to access the API, with the full API Key Data to get more functions

client = ClobClient( ####The client instance we will use for our python file.
  host,               # the polymarket host
  key=pk,            # your private key exported from magic/polymarket
  chain_id=chain_id,  # 137
  creds=api_key_data,        # your api creds (you can leave them empty if you havent generated/derivated them yet)
  signature_type=1,   # this type of config requires this signature type
  funder= os.getenv("POLYMARKET_FUND_ADDRESS")) # this is your polymarket public address (where you send the usdc)

### Function to save the dataframes
def save_df(dataframe, name):
    OUTPUT_DIR = '/opt/airflow/output'
    os.makedirs(OUTPUT_DIR, exist_ok=True) 

    df_copy = dataframe.copy()
    
    csv_file = name + ".csv"
    csv_path = posixpath.join(OUTPUT_DIR, csv_file)
    csv_path = os.path.normpath(csv_path).replace('\\', '/')
    
    try:
        dataframe.to_csv(csv_path, index=False, encoding='utf-8')
        print(f"Data has been written to {csv_path} successfully.")
        return csv_path  # show the path the file was saved in
    except Exception as e:
        print(f"Error saving file: {e}")
        raise  # Re-raise the exception after logging

### Function to upsert the Dataframe to PostgreSQL
### Upserting is when you don't delete previous data, and simply add newly generated data onto it.

def upsert_dataframe_to_postgres(df, table_name, conn, unique_keys):
    
    logger = logging.getLogger("airflow.task")

    # Check for integer columns exceeding PostgreSQL BIGINT limit
    bigint_limit = 9223372036854775807
    
    for col in df.columns:
        if pd.api.types.is_integer_dtype(df[col]):
            try:
                max_val = df[col].max()
                if pd.notnull(max_val) and max_val > bigint_limit:
                    logger.warning(f"Warning: Column '{col}' has a value too large for BIGINT: {max_val}")
            except Exception as e:
                logger.warning(f"Warning: Could not check column '{col}' for BIGINT overflow: {e}")
                
    ### I had a problem working with big integers, so this function will flag anytime a big integer is too big to work with on PostgreSQL
    
    columns = list(df.columns)
    
    values = [tuple(row) for row in df.to_numpy()]

    updates = ", ".join([f"{col} = EXCLUDED.{col}" for col in columns if col not in unique_keys])
    quoted_table_name = f'"{table_name}"'
    
    ### Here is the actual insert SQL statement that updates columns when there's a conflict on unique keys, but otherwise, simply adds the new data
    insert_statement = f"""
        INSERT INTO {quoted_table_name} ({', '.join(columns)})
        VALUES %s
        ON CONFLICT ({', '.join(unique_keys)})
        DO UPDATE SET {updates};
    """

    with conn.cursor() as cur:
        execute_values(cur, insert_statement, values)
    conn.commit()

### Here is a function to format arrays into either a None value, or a string value which can be converted later on. 
### Will be used later on when dealing with Tags in the Dataframe
def format_pg_array(value):
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    if isinstance(value, str):
        try:
            value = ast.literal_eval(value)
        except Exception:
            return None
    if not isinstance(value, list):
        return None
    return '{' + ','.join(f'"{str(tag)}"' for tag in value) + '}'

### Converting Arrays that are formatted like Dictionary strings into actual Arrays that are workable with on PostgreSQL
def parse_pg_array(pg_array_str):
    """
    Converts a PostgreSQL array string like '{"Bitcoin","Crypto"}' to a Python list ['Bitcoin', 'Crypto']
    """
    if not isinstance(pg_array_str, str) or not pg_array_str.startswith('{'):
        return []
    # This handles quoted values inside curly braces
    return re.findall(r'"([^"]*)"', pg_array_str)


### Following Functions returns a DF of all the Markets ever on Polymarket historically

def fetch_polymarket_data_list():
    ### Function to retrieve Polymarket data
    # Set up environment variables and constants
    ### This is to help set up the client function that we'll call upon (constant throughout the .py file)
    ### As well as setting up a function to retrieve the Market data (Which will be one of the main Polymarket tables we use - will act as an identifier table)
    
    host = "https://clob.polymarket.com"

    load_dotenv()
    private_key = os.getenv("PK")
    chain_id = POLYGON
    
    ### create list to store all markets available on polymarket
    all_markets = []
    page = 1
    next_cursor = None
    ### Next Cursor is a response we get from the get_market function we will use to retrieve our data
    ### it helps with numerating our data properly
    ### 
    client = ClobClient(host, key=pk, chain_id=chain_id, creds = api_key_data)
    
    try:
        # initialize the client
        
        start_time = datetime.now()
        
        while True:
            try:
                if page == 1:
                    response = client.get_markets()
                else:
                    response = client.get_markets(next_cursor=next_cursor)
                    
                if not response or not response.get('data'):
                    ### checking if the data has been retrieved in the response successfully, if so. print 
                    end_time = datetime.now()
                    duration = (end_time - start_time).total_seconds()
                    print(f"\nFetch completed successfully!")
                    print(f"Total markets retrieved: {len(all_markets)}")
                    print(f"Time taken: {duration:.3f} seconds")
                    print(f"Average rate: {len(all_markets)/duration:.2f} markets/second")

                    ### using this data to evaluate the performance of the function
                    break
                
                markets = response.get('data', [])
                
                if not markets:
                    ### checking if the data has been retrieved in the response successfully, if so. print 
                    end_time = datetime.now()
                    duration = (end_time - start_time).total_seconds()
                    print(f"\nFetch completed successfully!")
                    print(f"Total markets fetched: {len(all_markets)}")
                    print(f"Time taken: {duration:.2f} seconds")
                    print(f"Average rate: {len(all_markets)/duration:.1f} markets/second")
                    break
                    
                all_markets.extend(markets)
                print(f"Page {page}: Fetched {len(markets)} markets. Total: {len(all_markets)}")
                
                # Increment offset for next page
                next_cursor = response.get('next_cursor')
                page += 1
                
            except Exception as e:
                if "next item should be greater than or equal to 0" in str(e):
                    # This is actually a successful completion
                    end_time = datetime.now()
                    duration = (end_time - start_time).total_seconds()
                    print(f"\nFetch completed successfully!")
                    print(f"Total markets fetched: {len(all_markets)}")
                    print(f"Time taken: {duration:.2f} seconds")
                    print(f"Average rate: {len(all_markets)/duration:.1f} markets/second")
                    break
                else:
                    print(f"Error fetching page {page}: {str(e)}")
                    break

        return all_markets

    except Exception as e:
        print(f"Error initializing client: {str(e)}")
        return None

### This is a function to get all the Market data from Polymarket - Will be filtered for particular timeframes if requested (is_filtered)
def fetch_polymarket_data(is_filtered):
    markets_list = []
    df_markets = pd.DataFrame()
    csv_columns = set()
    next_cursor = None
    while True:
        try:
            # Print the cursor value for debugging if needed
            print(f"Fetching markets with next_cursor: {next_cursor}")
            # Making the API call based on the validity of the cursor value
            if next_cursor is None:
                response = client.get_markets()
            else:
                response = client.get_markets(next_cursor=next_cursor)
            if 'data' not in response:
                print("No data found in response.")
                break
            
            markets_list.extend(response['data'])
            ###adding the data to the markets_list list. 
            next_cursor = response.get("next_cursor")
            ###changing the cursor - will return none if no more data to fetch
    
            if not next_cursor:
                ### if next cursor is None, there is no more data to fetch
                break
        except Exception as e:
            # Print the exception details for debugging if needed
            print(f"Exception occurred: {e}")
            print(f"Exception details: {e.__class__.__name__}")
            print(f"Error message: {e.args}")
            break
    for market in markets_list:
        csv_columns.update(market.keys())
        # Also include nested keys like tokens
        if 'tokens' in market:
            csv_columns.update({f"token_{key}" for token in market['tokens'] for key in token.keys()})
    csv_columns = sorted(csv_columns)
    if is_filtered:
        markets_list_2025 = []
        for item in markets_list:
            date_str = item.get('end_date_iso')
            if not date_str:
                continue
            try:
                market_end_date = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%SZ")
                if market_end_date >= datetime.now():
                    markets_list_2025.append(item)
            except ValueError:
                continue
        markets_list = markets_list_2025.copy()
    try:
        rows = []
        for market in markets_list:
        ### looping through every market in the market list
            tokens = market.get('tokens', [])
            if all(token.get('token_id') == '' for token in tokens if isinstance(token, dict)):
                continue ### skipping a row if all token_ids are empty strings
            row = {}
            for key in csv_columns:
                if key.startswith("token_"):
                    token_key = key[len("token_"):]
                    row[key] = ', '.join([str(token.get(token_key, 'N/A')) for token in market.get('tokens', [])])
                elif key == "tokens":
                    row[key] = json.dumps(market.get("tokens", []), ensure_ascii=False)
                elif key == "rewards":
                    row[key] = json.dumps(market.get("rewards", {}), ensure_ascii=False)
                else:
                    row[key] = market.get(key, 'N/A')
            rows.append(row)
        
        df_markets = pd.DataFrame(rows, columns=csv_columns)
        df_markets = df_markets.replace('', None)

    except Exception as e:
        print(f"Error occurred: {e}")
    df_markets = df_markets.dropna(subset=["tokens", "rewards"])  # Drop rows with invalid JSON
    # Convert timestamp fields
    for col in ['accepting_order_timestamp', 'end_date_iso']:
        df_markets[col] = pd.to_datetime(df_markets[col], errors='coerce')
        df_markets[col] = df_markets[col].apply(lambda x: int(x.timestamp()) if pd.notnull(x) else None)
        df_markets[col] = (df_markets[col] // 1_000).astype('Int64')  # Store as seconds, re-multiply by 1000 if needed
        df_markets[col] = df_markets[col].round().astype('Int64')
    # Format tags for PostgreSQL text[] array insertion
    df_markets['tags'] = df_markets['tags'].apply(format_pg_array) ### Applying the format_pg_array function from earlier to make the column valid to work with

    return df_markets

### Function to return a list of token ids that are open on the Polymarket market

def filtering_open_markets_list(dataframe):
    open_token_ids = []
    ### iterating through each row in the dataframe
    for _, row in dataframe.iterrows():
        if row['accepting_orders']:
            for token in row['tokens']:
                if ('token_id' in token) and (token['token_id'] != ''):
                    open_token_ids.append(token['token_id'])
    return open_token_ids

### Function to return a df of token IDs that are open on the Polymarket market
def filtering_open_markets(dataframe):
    boolean = []
    ### iterating through each row in the dataframe
    for _, row in dataframe.iterrows():
        if row['accepting_orders']:
            boolean.append(True)
        else:
            boolean.append(False)
    open_markets_df = dataframe[boolean].reset_index(drop=True)
    # save_df(open_markets_df, name)
    return open_markets_df

### Function to filter Dataframes according to the tag column in the markets dataframe

def filter_DF_by_tags(tags: list, dataframe):
    tags = [str(tag).lower() for tag in tags]
    boolean = []

    for tags_str in dataframe['tags']:
        tag_list = [t.lower() for t in parse_pg_array(tags_str)]
        is_true = any(tag in tag_list for tag in tags)
        boolean.append(is_true)

    tagged_df = dataframe[boolean].reset_index(drop=True)
    return tagged_df

#### Function to convert the lists of orderbooksummaries into a comprehensive dataframe
    
def process_orderbooks(orderbooks: List[OrderBookSummary]) -> pd.DataFrame:
    
### Converting OrderbookSummary -> Dataframes so we can upload it to a database
    
### The dataFrame with columns: market, asset_id, timestamp, side, orderbooks

    data = []
    for orderbook in orderbooks:
        ### Isolating a singular orderbook - 
        pull_time = int(datetime.now().timestamp()) ###The timestamp when the entire pull was initiated

        ### Retrieving min timestamp between the bids / asks side of the orderbook (sometimes they differ slightly due to the api
        ### Gathering them into a list to find the minimum later on (makes the operation easier)
        bid_timestamps = [o.timestamp for o in orderbook.bids if hasattr(o, 'timestamp')]
        ask_timestamps = [o.timestamp for o in orderbook.asks if hasattr(o, 'timestamp')]

        # fallback to main orderbook timestamp if order timestamps aren't available
        timestamps_list = bid_timestamps + ask_timestamps
        timestamps = [int(t) for t in timestamps_list if t is not None]
        if timestamps:
            min_ts = min(timestamps)
        else:
            min_ts = orderbook.timestamp  # fallback
        
        bid_dict = {float(o.price): float(o.size) for o in orderbook.bids}
        ask_dict = {float(o.price): float(o.size) for o in orderbook.asks}

        ### Processing the bids
        data.append({
            'token_id': getattr(orderbook, 'token_id', 'N/A'),
            'market': orderbook.market,
            'asset_id': orderbook.asset_id,
            'timestamp_unix': min_ts,
            'retrieve_time' : pull_time, ### Timestamp defined at the beginning of the python file - indicates time the pull started.
            'side': 'bid',
            'orderbook': bid_dict,
            'hash': orderbook.hash
        })

        ### Processing the asks
        data.append({
            'token_id': getattr(orderbook, 'token_id', 'N/A'),
            'market': orderbook.market,
            'asset_id': orderbook.asset_id,
            'timestamp_unix': min_ts,
            'retrieve_time': pull_time, ### Timestamp defined at the beginning of the Python file - indicates time the pull started.
            'side': 'ask',
            'orderbook': ask_dict,
            'hash': orderbook.hash
        })
    ### The format is such that every single token ID with orders will have 2 rows, one showing the bids and one showing the asks.
    ### We will break down the columns even further in the Transformation phase of the ETL pipeline using SQL

    return pd.DataFrame(data)

def get_orderbooks_df(dataframe):
    ### Return a df of the order books of both the bid and ask sides of the market 
    ### input is a dataframe,and  the output is also a dataframe

    orderbooks_list = []

    for _, row in dataframe.iterrows():
        tokens = row['tokens']
        # If the tokens are stored as a JSON string, parse it back into a list

        if isinstance(tokens, str):
            try:
                tokens = json.loads(tokens)
            except json.JSONDecodeError:
                print(f"Skipping row due to JSON parse error in tokens: {tokens}")
                continue

        ### Skipping the row if the tokens column is not a list

        if not isinstance(tokens, list):
            continue

        for token in tokens:
            token_id = token.get('token_id')

            ### Skipping over instances where the token_id doesnt exist or the token_id is simply an empty string

            if not token_id:
                continue
            try:
                time.sleep(0.1) ### Adding a sleep function to limit the rate
                orderbook = client.get_order_book(token_id)
                if orderbook:
                    orderbook.token_id = token_id ### Manually modify the orderbook summary, this will help us form our dataframe later on
                    orderbooks_list.append(orderbook) ### Add the full orderbook to the list of orderbooks created when we initialized the function
            except Exception as e:
                print(f"Error fetching order book for token {token_id}: {str(e)}")
                continue

    orderbook_df = process_orderbooks(orderbooks_list)

    ### Processing the dataframe after it has been generated 

    orderbook_df = orderbook_df.replace('', None)
    
    ### Make the dictionaries into json strings for postgresql compatability

    orderbook_df['orderbook'] = orderbook_df['orderbook'].apply(
        lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, dict) else '{}'
    )

    ### Drop rows with NaN in timestamp_unix - PostgreSQL does not handle it extremely well.
    orderbook_df = orderbook_df.dropna(subset=['timestamp_unix'])
    orderbook_df = orderbook_df.dropna(subset=['retrieve_time'])
    
    ### Convert timestamp_unix and retrieve_time to numeric, forcing errors to NaN
    orderbook_df['timestamp_unix'] = pd.to_numeric(orderbook_df['timestamp_unix'], errors='coerce')
    # orderbook_df['retrieve_time'] = pd.to_numeric(orderbook_df['retrieve_time'], errors='coerce')

    ### Convert from milliseconds to seconds and cast to type Int64
    orderbook_df['timestamp_unix'] = (orderbook_df['timestamp_unix'] / 1000).round().astype('Int64')

    orderbook_df['token_id'] = orderbook_df['token_id'].astype(str)
    orderbook_df['asset_id'] = orderbook_df['asset_id'].astype(str)
    orderbook_df['hash'] = orderbook_df['hash'].astype(str)

    return orderbook_df

### Function to pull the Binance Orderbooks for any currency pair (usually BTC USDC)
def pull_binance_orderbook(pair):
    pull_time = int(datetime.now().timestamp()) ### The timestamp when the entire pull was initiated
    orderbook = binance_client.depth(symbol=pair)
    rows = []
    for side in ['bids','asks']:
        for entry in orderbook.get(side,[]):
            price, volume = entry
            rows.append({
            'currency_pair':pair,
            'retrieve_time': pull_time, ### Timestamp defined at the beginning of the Python file - indicates time the pull started.
            'side': 'bid' if side == 'bids' else 'ask',
            'price': float(price),
            'volume': float(volume)
            })
    df = pd.DataFrame(rows, columns = ['currency_pair', 'retrieve_time', 'side', 'price', 'volume'])
    return df
    