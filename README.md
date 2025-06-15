# Polymarket_DE_Project
A Data Engineering project. Using the Polymarket and Binance APIs to pull data into a local database. Afterwards, using said data to build numerous useful tables that users can quickly query. Tools used: Python, Airflow, Docker, PostgreSQL, Streamlit



# Synopsis 

The purpose of this data engineering project is to provide users a centralized view on *what* is going on in Polymarket at any given time. Users are able to pull Polymarket data themselves to quickly compare and contrast what is happening in various betting markets and infer their own decisions based on their analysis. Furthermore, this pipeline is also able to provide a rough sentiment analysis on the general Cryptomarket. More specifically, by analyzing Polymarket's betting activity on the future movements of Bitcoin, we can infer the general market sentiment for Bitcoin/Crypto across various time frames. This pipeline provides users with *automated data collection*, PostgreSQL-based storage and transformation, and an exploratory dashboard for exploring Crypto sentiment, Arbitrage opportunities, and price spreads. 


## Features Outline

# 1 - The ETL Pipeline: 

## Architecture Diagaram:
![data architecture june 15](https://github.com/user-attachments/assets/299e5675-d50b-42c3-9a7f-525d31617ee6)


Above, you can see the general flow of the project. Nelow, I have detailed all the relevant steps:

#Step I - Extraction

The first step was *extracting* the relevant data from the Binance and Polymarket APIs. These are both public APIs created and managed by the enterprises, respectively. I used a Python Script to extract the data.

#Step II - Transform

The second step was transforming the data. After the initial Python script to extract the raw data from Binance and Polymarket was created, I used Airflow as an orchestration tool. The purpose of Airflow is to establish a DAG (Directed acyclic graph). The purpose of a DAG is to define specifically which tasks should be run in which order. Within the DAG I specified, I ran both Python and SQL transformations on the raw data, so I could create numerous useful tables for the end-user to use.

#Step III - Load

Finally, after the DAG was established, I was ready to load all of the data into PostgreSQL. I used DBeaver to access the database locally on my computer. However, this project can be migrated to cloud storage. 

Once the tables were loaded into my local database, I was then able to create a small visualization layer for the end user to explore the data. For the visualization layer, I used Streamlit. However, users trying to attempt this project can also use other visualization tools such as Looker and Tableau. I also used Ngrok to host the Streamlit online, while the data is hosted strictly on my local computer. 
