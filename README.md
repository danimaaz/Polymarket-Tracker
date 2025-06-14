# Polymarket_DE_Project
A Data Engineering project. Using the Polymarket and Binance APIs to pull data into a local database. Afterwards, using said data to build numerous useful tables that users can quickly query. Tools used: Python, Airflow, Docker, PostgreSQL, Streamlit



# Synopsis 

The purpose of this data engineering project is to provide users a centralized view on *what* is going on in Polymarket at any given time. Users are able to pull Polymarket data themselves to quickly compare and contrast what is happening in various betting markets and infer their own decisions based on their analysis. Furthermore, this pipeline is also able to provide a rough sentiment analysis on the general Cryptomarket. More specifically, by analyzing Polymarket's betting activity on the future movements of Bitcoin, we can infer the general market sentiment for Bitcoin/Crypto across various time frames. This pipeline provides users with *automated data collection*, PostgreSQL-based storage and transformation, and an exploratory dashboard for exploring Crypto sentiment, Arbitrage opportunities, and price spreads. 


## Features Outline

# 1 - The ETL Pipeline: 

