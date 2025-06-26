 Polymarket_DE_Project
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


# 3 - Dockerized Environment

This project is containerized using Docker Compose. Containerization refers to the process of packaging applications and their corresponding dependencies into a virtual environment. The purpose of containerizing a particular project is to mitigate the risk that the project will not work in one environment but will in another (e.g., the project might work on my computer but not another person's computer). 

The Dockerized environment for this project contains the following components:

 - Airflow: An Orchestration tool that enables the scheduling and execution of the data pipeline
 - PostgreSQL: A database container that stores all the raw and transformed data from the relevant APIs
 - Streamlit: A frontend service that enables us to launch the Polymarket Exploratory dashboard via a web interface. 

# 4 - Streamlit Integration

Finally, to complete the end-to-end nature of this project, this project also includes a visualization element in Streamlit. More specifically, the dashboard provides a quick view for visualizing Bitcoin market sentiment and prediction market analytics. A snapshot of the dashboard is below:

<img width="1250" alt="image" src="https://github.com/user-attachments/assets/f415f51a-c6e0-4113-9f96-24ccea9f165e" />

The dashboard is split into 5 sections:

- **Sentiment Gauge**: This displays a score on the short, medium, and long-term sentiment on Bitcoin price movements on a scale from 1-100.
  - The way the Sentiment Score is predicted is based on the following formulas:
   - Firstly, we separate the different 'predictions' available in Polymarket into two main groups, Bullish (where predicted price > current price) and Bearish (where predicted price < current price).
   - We also separate the various markets on whether they are predicting the price in the short term (price within <= 7 days), Medium term (price within <= 30 days and > 7 days), or long term (price within > 30 days). 
   - Then, we calculate what the Bullish and Bearish scores are using the following formula (note that the P(Reaching Target Price) is assumed to be the current price on Polymarket of BTC hitting the target price):
     <img width="508" alt="image" src="https://github.com/user-attachments/assets/e4fb6096-7bda-41e8-b38c-2a5bf6fa795c" />

    - Afterwards, we calculate the overall sentiment score for a particular time-frame using the following formula:
      <img width="445" alt="image" src="https://github.com/user-attachments/assets/da7da360-25b8-45f5-8d4a-f16a700c4316" />
    - Note that the default sentiment score starts at 50 because 50 is defined to be perfectly neutral in our gauge. A score of 1 is defined to be extremely bearish, while 100 is defined to be extremely Bullish. 


 
- **Arbitrage Opportunities**: This displays markets at the time of the latest refresh with mispriced markets. More specifically, if the price of the two outcomes (usually defined as Yes and No) is less than $1. Then, there is an arbitrage opportunity as users can buy both sides of the market and make a guaranteed profit.
- **Token Movers**: This view ranks the various markets (also labeled as tokens) wth the largest price movements since the last refresh.
- **Market Spreads**: This view shows the markets with the largest bid-ask spreads. Large bid-ask spreads are usually an indicator of illiquidity or extreme uncertainty.
- **Closing Markets**: This view displays markets that are set to conclude soon due to their time sensitivity.

The Python tools used to make this dashboard were:
- **Matplotlib**: A common  tool that was used to create the Sentiment graph.
- **Pandas**: Used on the back-end for data-processing as well as plotting the interactive tables seen on the dashboard.

  
 ## Documentation

 # 1 - SQL Table Documentation

 
# polymarket_market_data_full

|  Column Name      | Description   | Data Type  | Unique Key? |
|:-------------:|:-------------:| :---------:| :---------:|
| accepting_order_timestamp      | When the specific market accepted orders for the first time (denoted in Unixtime)| bigint | No |
| accepting_orders      | Is the market currently accepting orders?     |   boolean |   No |
| active | Is the market currently active?     |   boolean |   No |
| archived | Is the market currently archived?     |   boolean |   No |
| condition_id | The identifier of a specific question *and* its' possible outcomes    |   string |   Yes |
| description | Is the market currently active?     |   boolean |   No |
| enable_order_book  | is the orderbook enabled? |   string |   No |
| end_date_iso  | when is the market set to close? |   bigint |   No |
| fpmm | address of associated fixed product market maker on Polygon network |   string |   No |
| game_start_time | For sports betting, it is when the games are scheduled to start     |   string |   No |
| icon | link to the icon of the market |   string |   No |
| image | link to the image of the market (usually same as icon)     |   string |   No |
| is_50_50_outcome  | are there only two outcomes? |   boolean |   No |
| maker_base_fee | what fee is charged for limit orders (usually 0)      |   float |   No |
| market_slug | The end of the URL for the specific polymarket market      |   string |   No |
| minimum_order_size  | minimum limit order size     |   integer |   No |
| minimum_tick_size  | minimum tick size in units of implied probability (price on market)     |   float4 |   No |
| neg_risk | is there another market that can affect this current market? |   Boolean |   No |
| neg_risk_market_id    | If neg_risk is True, what is the market_id of said market that affects this current market |   string |   No |
| neg_risk_request_id   | If neg_risk is True, what is the request_id of said market that affects this current market |   string |   No |
| notifications_enabled | does the user who's accessing this API have notifications enabled for this specific question/market? |   Boolean |   No |
| question | the question the market is asking (i.e. who will win X game, will bitcoin reach Y price, etc) |   string |   No |
| question_id | the unique identifier for the question text |   string |   Yes |
| Rewards | the array of potential rewards for users who provide liquidity to smaller markets |   Array/jsonb |   No |
| seconds_delay  | seconds of match delay for in-game trade (sports)|  int4 |   No |
| Tags | the array of various Tags/topics that apply to a specific question_id (ex: sports, bitcoin, politics, etc) |   Array/jsonb |   No |
| taker_base_fee | Always 0, but it showed the fee applied to market takers (people who take away liquidity by buying and holding positions) |   int4 |   No |
| token_outcome  | (Deprecated) A string list of the possible outcomes. Ex: Yes, No |   string |   No |
| token_price  | A string list of the corresponding price from token_outcome col. ex: if token_outcome is Yes, No - and token_price = 0.6, 0.4. Then Yes = $0.6. |   string |   No |
| token_token_id  | A string list of the respective token ids of the outcomes |   string |   No |
| token_winner  | A string list on if the respective outcome/token_id has been concluded and is declared a winner (all False means still ongoing) |  string |   No |
| tokens  | jsonb column of the different tokens within a market/question_id, including the potential outcomes, price, token_id, and if they are the market winner |   Array/jsonb |   No |




# polymarket_orderbooks_data_full



|  Column Name      | Description   | Data Type  | Unique Key |
|:-------------:|:-------------:| :---------:| :---------:|
| token_id  | A unique identifier of the question_id + outcome combination. |   string |   Yes |
| market  | A unique identifier of the question_id/market (also called market_id). |   string |   No |
| asset_id  | Id of the asset/token (usually the same as token_id) |   string |   No |
| timestamp_unix | the precise timestamp the specific orderbook was pulled |   bigint |   Yes |
| retrieve_time | the timestamp the initial request to pull the orderbook was made (same for all markets pulled during the same run) |   bigint |   No |
| side  | bid or ask side |   string |   Yes |
| orderbook  | Full jsonb/dictionary of the various price points and the respective volume in the orderbook. |   jsonb |   No |
| hash  | Hash summary of the orderbook content. |   string |   No |


# Binance_orderbook_Full


|  Column Name     | Description   | Data Type  | Unique Key |
|:-------------:|:-------------:| :---------:| :---------:|
| currency_pair  | The currency pair that is being monitored on Binance (for our purposes, it is always BTCUSDC) |   string |   Yes |
| retrieve_time   | The time which the initial orderbook pull was requested (unixtime format) |   bigint |   Yes |
| side | bid/ask side of the market |   string |   Yes |
| price  | The price of the current order entry in the orderbook |   float |   Yes |
| volume  | The total volume (in BTC) of the orders in that particular entry of the orderbook |   float |   No |


#price_tracker_table

|  Column Name      | Description   | Data Type  | Unique Key |
|:-------------:|:-------------:| :---------:| :---------:|
| question_id | the unique identifier for the question text |   string |   Yes |
| question | the question the market is asking (i.e. who will win X game, will bitcoin reach Y price, etc) |   string |   No |
| Tags | the array of various Tags/topics that apply to a specific question_id (ex: sports, bitcoin, politics, etc) |   Array/jsonb |   No |
| outcome | the outcome (usually binary, yes/no, sports team, athlete, etc) |   string |   No |
| winner | Is this outcome the 'winner' of the market at the time of querying? |   Boolean |   No |
| token_id  | A unique identifier of the question_id + outcome combination. |   string |   Yes |
| market_id  | A unique identifier of the general market (slightly different from the question_id) |   string |   No |
| asset_id  | A unique identifier of the question_id + outcome combination (same as token_id) |   string |   No |
| retrieve_time | the timestamp the initial request to pull the orderbook was made (same for all markets pulled during the same run) |   timestamp |   Yes |
| ask_price | the lowest ask price (in USD) of the token in question |   float |   No |
| bid_price | the highest bid price (in USD) of the token in question |   float |   No |
| market_spread| the difference between the ask_price & bid_price columns |   float |   No |
| bid_shares | the total volume of shares available on the bid side of the orderbook |   int |   No |
| ask_shares | the total volume of shares available on the ask side of the orderbook |   int |   No |
| list_price | the middle point between the lowest ask price and highest bid price in the orderbook |   float |   No |

# Detecting_arbitrage_table

|  Column Name      | Description   | Data Type  | Unique Key |
|:-------------:|:-------------:| :---------:| :---------:|
| question_id | the unique identifier for the question text |   string |   Yes |
| question | the question the market is asking (i.e. who will win X game, will bitcoin reach Y price, etc) |   string |   No |
| Tags | the array of various Tags/topics that apply to a specific question_id (ex: sports, bitcoin, politics, etc) |   Array/jsonb |   No |
| market_id  | A unique identifier of the general market (slightly different from the question_id) |   string |   No |
| retrieve_time | the timestamp the initial request to pull the orderbook was made (same for all markets pulled during the same run) |   timestamp |   Yes |
| market_end_date | the timestamp at which the market is planned to close at |   timestamp |   No |
| outcomes | the possible outcomes to this market (usually binary, yes/no, team 1 vs team 2, etc, occasionally three outcomes if draws are permitted) |   Array |   No |
| no_price | the lowest ask price of the 'no' outcome in a particular market (if it is a sports game, it'll be the price of the first listed outcome in the outcomes column) |   float |   No |
| yes_price | the lowest ask price of the 'yes' outcome in a particular market (if it is a sports game, it'll be the price of the second listed outcome in the outcomes column) |   float |   No |
| yes_instances | Sanity check column - counting the number of yes/no market instances - to prevent duplication or grabbing the wrong rows. This should always be 1  |   int |   No |
| no_instances | Sanity check column - counting the number of yes/no market instances - to prevent duplication or grabbing the wrong rows. This should always be 1   |   int |   No |
| is_arbitrage| Indicator function, returns 1 if the yes_price + no_price are less than $1 (i.e. arbitrage = True), otherwise, returns 0. |   int |   No |

