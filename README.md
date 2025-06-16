# Polymarket_DE_Project
A Data Engineering project. Using the Polymarket and Binance APIs to pull data into a local database. Afterwards, using said data to build numerous useful tables that users can quickly query. Tools used: Python, Airflow, Docker, PostgreSQL, Streamlit



# Synopsis 

The purpose of this data engineering project is to provide users a centralized view on *what* is going on in Polymarket at any given time. Users are able to pull Polymarket data themselves to quickly compare and contrast what is happening in various betting markets and infer their own decisions based on their analysis. Furthermore, this pipeline is also able to provide a rough sentiment analysis on the general Cryptomarket. More specifically, by analyzing Polymarket's betting activity on the future movements of Bitcoin, we can infer the general market sentiment for Bitcoin/Crypto across various time frames. This pipeline provides users with *automated data collection*, PostgreSQL-based storage and transformation, and an exploratory dashboard for exploring Crypto sentiment, Arbitrage opportunities, and price spreads. 


## Features Outline

# 1 - The ETL Pipeline: 

## Architecture Diagram:
![data architecture june 15](https://github.com/user-attachments/assets/299e5675-d50b-42c3-9a7f-525d31617ee6)


Above, you can see the overall project flow. Below, I have detailed all the relevant steps:

#Step I - Extraction

The first step was *extracting* the relevant data from the Binance and Polymarket APIs. These are both public APIs created and managed by the enterprises, respectively. I used a Python Script to extract the data.

#Step II - Transform

The second step was transforming the data. After the initial Python script to extract the raw data from Binance and Polymarket was created, I used Airflow as an orchestration tool. The purpose of Airflow is to establish a Directed Acyclic Graph (DAG). The purpose of a DAG is to define specifically which tasks should be run in which order. Within the DAG I specified, I ran both Python and SQL transformations on the raw data, so I could create numerous useful tables for the end-user to use.

#Step III - Load

Finally, after the DAG was established, I was ready to load all of the data into PostgreSQL. I used DBeaver to access the database locally on my computer. However, this project can be migrated to cloud storage. 

Once the tables were loaded into my local database, I was then able to create a small visualization layer for the end user to explore the data. For the visualization layer, I used Streamlit. However, users attempting this project can also use other visualization tools, such as Looker and Tableau. I also used Ngrok to host the Streamlit online, while the data is hosted strictly on my local computer. 

# 2 - API Integration

What is the purpose of using specifically Binance and Polymarket data? 

The idea for this project initially was to create a rough Bitcoin sentiment graph. So I can see the general market sentiment on Bitcoin. Polymarket is a crypto-based prediction market. Meaning that users buy or sell futures that hinge on a particular event happening. This effectively makes it a bookmaker. The way prices are quoted (1 share yielding $1 if successful) deems it a good substitute for the *probability* that an event will happen. Thus, I integrated the Polymarket API to primarily get this data. 

I integrated the Binance API data to get real-time data on how the orderbook of BTC USDC is moving. Using that data, I was able to classify whether a particular prediction market is bullish or bearish. 

# 3 - PostgreSQL Storage

PostgreSQL is an open-source *relational* database that is being used as this project's data storage layer. It was chosen due to its ability to support the structured data coming from the APIs being used, its ability to handle JSON-type columns, and the low cost associated with it. 

All the raw and transformed data from this project gets loaded onto a *local* PostgreSQL storage container. 

## Stored Tables Overview

Raw Data: 

The backbone of this project is based on the loading of these three raw tables:

polymarket_market_data_raw: This table is primarily an information table containing details on all the markets, both past and present, listed on Polymarket.

polymarket_orderbooks_data_raw: This table is a *snapshot* in time of all the Orderbooks currently available in Polymarket.

binance_orderbook_raw: This table is a *snapshot* in time of the BTC USDC orderbook from Binance. 


Using the 3 raw data tables, we can transform them into multiple useful tables for stakeholders to use.

Transformed Data: 


polymarket_market_data_full: A functional copy of polymarket_market_data_raw where the columns are converted into usable SQL formats.

polymarket_orderbooks_data_full: A functional copy of polymarket_orderbooks_data_raw where the columns are converted into usable SQL formats.

binance_orderbook_full: A functional copy of binance_orderbook_raw where the columns are converted into usable SQL formats.

polymarket_orders_full: Joining the Polymarket_market_data (information table) with the Polymarket orders data. This way, people can query only one table to get information on the orderbook and immediately see the important information on the market. 

price_tracker_table: This table simplifies the Polymarket orderbook data from before. Order books tend to have dozens of bid (people who want to buy) and ask (people who want to sell) orders. The price tracker table simplifies this data by returning only 1 row per market per timestamp, where you can easily see what the best bid and ask prices are. 

detecting_arbitrage_table: Given that Polymarket data is a betting market. There are sometimes instances of slight arbitrage. More specifically, when the sum of the prices of an outcome is less than $1, then users can buy all sides of the market and make a guaranteed profit. For example, if there is a particular sports match going on (Ex: Knicks vs Celtics), and the price of wagering the Knicks or Celtics will win is $0.7 and $0.2, respectively, then a user can buy both sides of the market and still come out on top. The return for a winning outcome is $1, which is greater than $0.7 + $0.2. This table tracks all instances of arbitrage opportunities on the market at the time the Data is pulled. 

The specific relations between each table and the documentation on the columns will be included in ____ file of the project. 
