import psycopg2
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.sql import text 
from sqlalchemy.engine import Engine
import sqlalchemy
from datetime import datetime
from datetime import datetime, timedelta
import re
import pandas as pd
from datetime import datetime, timedelta
import re
import matplotlib.pyplot as plt
import numpy as np
import streamlit as st
import os
from packaging import version
import os

db_url = os.getenv("DATABASE_URL", "postgresql+psycopg2://airflow:airflow@postgres:5432/airflow")

### Raising an error if there is no database env. variable set, meaning we can't connect to PostgreSQL
if not db_url:
    raise ValueError("DATABASE_URL environment variable is not set!")

engine = create_engine(db_url, future=True)
### Creating a function where we connect to PostgreSQL using a raw connection (normal connection doesn't work due to version issues (to remain consistent across all python files)
def read_sql_function(query, engine):
    raw_conn = engine.raw_connection()
    try:
        return pd.read_sql(query, raw_conn)
    finally:
        raw_conn.close()

### Query to retrieve the BTC price from Binance 

btc_price_query = """
with price_per_ts as 
(
select retrieve_time, 
min(case when side = 'ask' then price else null end) as ask_price, 
max(case when side = 'bid' then price else null end) as bid_price
from binance_orderbook_full 
group by 1
order by retrieve_time desc
)
select
to_timestamp(retrieve_time) as orderbook_ts,
round(((ask_price+bid_price)/2)::numeric, 2) as price
from price_per_ts
order by 1 desc
limit 1
"""

#### Query to retrieve the price of the BTC Markets on Polymarket 

market_query = """
SELECT question, 
       tags, 
       outcomes[1] as first_outcome,
       market_end_date,
       yes_price AS yes_probability
FROM detecting_arbitrage_table
WHERE retrieve_time = (
    SELECT MAX(retrieve_time) FROM detecting_arbitrage_table
)
AND (
    question ILIKE '%Will the price of Bitcoin be %'
    OR question ILIKE '%Bitcoin Up or Down%'
    OR question ILIKE '%Will Bitcoin%'
    OR 
    (
        question ILIKE '%bitcoin%' AND 
        (
            question ILIKE '%in January%' OR
            question ILIKE '%in February%' OR
            question ILIKE '%in March%' OR
            question ILIKE '%in April%' OR
            question ILIKE '%in May%' OR
            question ILIKE '%in June%' OR
            question ILIKE '%in July%' OR
            question ILIKE '%in August%' OR
            question ILIKE '%in September%' OR
            question ILIKE '%in October%' OR
            question ILIKE '%in November%' OR
            question ILIKE '%in December%'
        )
    )
)
AND NOT (
    question ILIKE '%PM ET%'
    OR question ILIKE '%AM ET%'
		)
"""

### Query to retrieve all the inefficient markets on Polymarket (meaning they have arbitrage)

arbitrage_query = """
SELECT 
question, 
tags, 
yes_price, 
no_price, 
(1 - (yes_price + no_price)) * 100 AS guaranteed_profit_percentage
FROM detecting_arbitrage_table
WHERE 
retrieve_time = (SELECT MAX(retrieve_time) FROM detecting_arbitrage_table)
AND 
is_arbitrage = 1
ORDER BY guaranteed_profit_percentage DESC
"""

### Query to retrieve the Polymarket markets w/ the biggest price fluctuations

movers_query = """
WITH token_movers AS (
    SELECT 
        question_id,
        question,
        tags,
        token_id,
        outcome,
        list_price,
        retrieve_time,
        LAG(list_price) OVER (PARTITION BY token_id ORDER BY retrieve_time) AS prev_list_price
    FROM price_tracker_table
    WHERE retrieve_time IN (
        SELECT DISTINCT retrieve_time
        FROM price_tracker_table
        ORDER BY retrieve_time DESC
        LIMIT 2
    )
),
latest_data AS (
    SELECT 
        tm.question_id,
        tm.question,
        tm.token_id,
        tm.tags,
        tm.outcome,
        tm.list_price AS current_price,
        tm.prev_list_price,
        (tm.list_price - tm.prev_list_price) AS price_change,
        ((tm.list_price - tm.prev_list_price) / tm.prev_list_price) * 100 AS pct_change
    FROM token_movers tm
    WHERE tm.prev_list_price IS NOT NULL AND tm.list_price IS NOT NULL
)
SELECT *
FROM latest_data
"""

### Query to retrieve the Polymarket markets w/ the biggest bid-ask spreads

spread_query = """
SELECT 
question, 
outcome,
tags, 
ask_price, 
bid_price, 
(ask_price - bid_price) AS spread,
((ask_price - bid_price) / ((ask_price + bid_price)/2)) * 100 AS spread_pct,
bid_shares,
ask_shares
FROM price_tracker_table
WHERE 
retrieve_time = (SELECT MAX(retrieve_time) FROM price_tracker_table)
and 
ask_price is not null 
and 
bid_price is not null
ORDER BY spread_pct DESC
LIMIT 10

"""
### Query to retrieve the Polymarket markets ending soon 

closing_query = """
SELECT 
    question,
    tags,
    market_end_date,
    EXTRACT(EPOCH FROM (market_end_date - NOW())) / 3600 AS hours_until_close 
FROM polymarket_orders_full
WHERE 
    EXTRACT(EPOCH FROM (market_end_date - NOW())) > 0
    AND retrieve_time = (SELECT MAX(retrieve_time) FROM polymarket_orders_full)
GROUP BY 1, 2, 3, 4
ORDER BY market_end_date ASC
"""

# Get max retrieve_time from detecting_arbitrage_table - to see the last DAG refresh time

max_polymarket_ts_query = "SELECT MAX(retrieve_time) as max_retrieve_time FROM detecting_arbitrage_table"

market_df         = read_sql_function(market_query, engine)
current_price_df  = read_sql_function(btc_price_query, engine)
arb_df            = read_sql_function(arbitrage_query, engine)
movers_df         = read_sql_function(movers_query, engine)
spread_df         = read_sql_function(spread_query, engine)
closing_df        = read_sql_function(closing_query, engine)
max_polymarket_ts = read_sql_function(max_polymarket_ts_query, engine).iloc[0, 0]


current_btc_price = float(current_price_df.iloc[0]["price"])

### Function to label each BTC market on Polymarket as either Short Term, Medium Term, or Long Term
def label_time_frame(row):
    q = row['question'].lower()
    if "up or down" in q:
        return "Ignore"
    if "purchase bitcoin" in q:
        return "Ignore"
    
    # Remove timezone if present
    market_end = row['market_end_date']
    if pd.notnull(market_end) and hasattr(market_end, 'tzinfo'):
        market_end = market_end.tz_localize(None)
    
    delta = (market_end - datetime.now()).days
    if delta < 7:
        return "Short-term"
    elif delta <= 30:
        return "Medium-term"
    else:
        return "Long-term"

market_df['time_frame'] = market_df.apply(label_time_frame, axis=1)

market_df['time_frame'] = market_df.apply(label_time_frame, axis=1)

### Parsing out the targets in every single BTC Polymarket market (From the various texts) 
def parse_target(row):
    q = row['question']
    tf = row['time_frame']
    if tf == "Ignore":
        return 0
    ### When the price is denoted in $__K, this function is to normalize and have it as a concrete target 
    def normalize_price(val):
        val = val.replace(",", "").replace("$", "")
        if 'K' in val.upper():
            return int(float(val.upper().replace("K", "")) * 1000)
        else:
            return int(float(val))

    # "dip" or "reach"
    if "dip to $" in q.lower() or "reach $" in q.lower():
        match = re.search(r"\$\s?([\d,]+K?)", q)
        if match:
            return normalize_price(match.group(1))

    # "less than"
    if "be less than $" in q.lower():
        match = re.search(r"less than \$([\d,]+K?)", q, re.IGNORECASE)
        if match:
            return normalize_price(match.group(1)) - 1

    # "greater than"
    if "be greater than $" in q.lower():
        match = re.search(r"greater than \$([\d,]+K?)", q, re.IGNORECASE)
        if match:
            return normalize_price(match.group(1)) + 1

    # "between $X and $Y"
    if "be between $" in q.lower():
        match = re.search(r"between \$([\d,]+K?) and \$([\d,]+K?)", q, re.IGNORECASE)
        if match:
            t1 = normalize_price(match.group(1))
            t2 = normalize_price(match.group(2))
            return int((t1 + t2) / 2)

    return -1  # Catch anything not matched

market_df['Target'] = market_df.apply(parse_target, axis=1)


### For each timeframe, using the formula to determine the general BTC Sentiment from the Polymarket markets
def calculate_sentiment_by_timeframe(df, current_btc_price):
    results = {}

    # Percent movement threshold by timeframe
    percent_thresholds = {
        'Short-term': 0.25,
        'Medium-term': 0.5,
        'Long-term': 2

    }

    def score_markets(sub_df, tf):
        bullish_score = 0
        bearish_score = 0
        total_weight = 0

        for _, row in sub_df.iterrows():
            target = row['Target']
            prob = row['yes_probability']

            if pd.isnull(prob) or target <= 0:
                continue

            # % difference from current BTC price
            percent_move = abs(target - current_btc_price) / current_btc_price * 100

            # Skip if the move is too small for this timeframe
            if percent_move < percent_thresholds[tf]:
                continue

            # Weight: how far the move is, times the confidence
            weight = percent_move * prob

            if target > current_btc_price:
                bullish_score += weight
            elif target < current_btc_price:
                bearish_score += weight

            total_weight += weight

        if total_weight == 0:
            return 50  # Neutral if no useful data

        raw_score = 50 + 50 * (bullish_score - bearish_score) / total_weight
        return max(1, min(100, round(raw_score)))

    for tf in ['Short-term', 'Medium-term', 'Long-term']:
        sub_df = df[df['time_frame'] == tf]
        results[tf] = score_markets(sub_df, tf)

    return results
sentiment_scores = calculate_sentiment_by_timeframe(market_df, current_btc_price)

binance_dt = current_price_df.iloc[0]["orderbook_ts"]

# Get max retrieve_time from detecting_arbitrage_table, label it as the binance_dt otherwise, so handling 
if max_polymarket_ts is None:
    last_refresh_time = binance_dt
else:
    polymarket_dt = pd.to_datetime(max_polymarket_ts, unit='ms')
    last_refresh_time = max(binance_dt, polymarket_dt)
last_refresh_time_str = last_refresh_time.strftime("%Y-%m-%d %H:%M:%S") + " UTC"


### Function to plot the speedometer on Streamlit and labeling each score in a particular subsection (Bearish - Bullish)
def plot_speedometer(score, title, ax):
    labels = ['Strongly Bearish', 'Bearish','Slightly Bearish', 'Neutral', 'Slightly Bullish', 'Bullish', 'Strongly Bullish']
    colors = ['red', 'coral', 'orange', 'yellow', 'greenyellow', 'lightgreen', 'green']
    zones = [0, 15, 30, 45, 55, 70, 85, 100]
    major_ticks = [0, 15, 30, 45, 55, 70, 85, 100]

    def get_label(score):
        if zones[0] <= score < zones[1]:
            return labels[0]
        elif zones[1] <= score < zones[2]:
            return labels[1]
        elif zones[2] <= score < zones[3]:
            return labels[2]
        elif zones[3] <= score < zones[4]:
            return labels[3]
        elif zones[4] <= score < zones[5]:
            return labels[4]
        elif zones[5] <= score < zones[6]:
            return labels[5]
        elif zones[6] <= score <= zones[7]:
            return labels[6]
        else:
            return "Invalid"

    # Draw colored arcs
    for i in range(len(zones) - 1):
        theta = np.linspace(
            180 - (zones[i] / 100 * 180),
            180 - (zones[i + 1] / 100 * 180),
            100
        )
        ax.plot(
            np.cos(np.radians(theta)),
            np.sin(np.radians(theta)),
            lw=15,
            solid_capstyle='butt',
            color=colors[i]
        )

    # Draw pointer
    pointer_angle = 180 - (score / 100 * 180)
    ax.arrow(
        0, 0,
        0.5 * np.cos(np.radians(pointer_angle)),
        0.5 * np.sin(np.radians(pointer_angle)),
        width=0.02,
        head_width=0.05,
        head_length=0.3,
        fc='black',
        ec='black'
    )

    # Draw tick marks
    for tick in major_ticks:
        angle = 180 - (tick / 100 * 180)
        x_outer = 1.05 * np.cos(np.radians(angle))
        y_outer = 1.05 * np.sin(np.radians(angle))
        x_inner = 0.85 * np.cos(np.radians(angle))
        y_inner = 0.85 * np.sin(np.radians(angle))
        ax.plot([x_inner, x_outer], [y_inner, y_outer], color='black', lw=1.25)
        ax.text(
            1.15 * np.cos(np.radians(angle)),
            1.15 * np.sin(np.radians(angle)),
            str(tick),
            ha='center', va='center', fontsize=10
        )

    # Add custom transparent tick at score
    if score not in major_ticks:
        angle = 180 - (score / 100 * 180)
        x_outer = 1.05 * np.cos(np.radians(angle))
        y_outer = 1.05 * np.sin(np.radians(angle))
        x_inner = 0.9 * np.cos(np.radians(angle))
        y_inner = 0.9 * np.sin(np.radians(angle))
        ax.plot([x_inner, x_outer], [y_inner, y_outer], color='black', lw=1.25, alpha=0.5)
    
        # If you want to re-enable the score label, uncomment below:
        # ax.text(
        #     1.15 * np.cos(np.radians(angle)),
        #     1.15 * np.sin(np.radians(angle)),
        #     str(score),
        #     ha='center', va='center', fontsize=10, alpha=0.5
        # )

    # Show numerical score in center (currently disabled)
    ax.text(0, -0.2, f"{score}/100", fontsize=14, fontweight='bold', ha='center', va='center')

    # Show sentiment label just below the (now-hidden) score
    label = get_label(score)
    ax.text(0, -0.35, label, fontsize=12, fontstyle='italic', ha='center', va='center', color='gray')

    # Raise the title above the plot
    ax.text(0, 1.3, title, fontsize=14, fontweight='bold', ha='center')

    ax.set_aspect('equal')
    ax.axis('off')
fig, axs = plt.subplots(1, 3, figsize=(24, 14))

fig.text(
    0.95,  # Right edge (95% from left)
    0.95,  # Top edge (95% from bottom)
    f"Last Refresh: {last_refresh_time_str}",
    ha='right',  # Right-align text
    va='top',    # Align to top
    fontsize=20,
    color='gray',
    fontstyle='italic',
    bbox=dict(facecolor='white', alpha=0.7, edgecolor='none', boxstyle='round,pad=0.5')  # Add subtle background
)


plot_speedometer(sentiment_scores['Short-term'], 'Short-term Sentiment', axs[0])
plot_speedometer(sentiment_scores['Medium-term'], 'Medium-term Sentiment', axs[1])
plot_speedometer(sentiment_scores['Long-term'], 'Long-term Sentiment', axs[2])
st.set_page_config(layout="wide")
st.markdown("<h1 style='text-align: center;'>BTC Sentiment Indicator (using betting data)</h1>", unsafe_allow_html=True)
st.pyplot(fig)


closing_df['market_end_date'] = (
    closing_df['market_end_date']
    .dt.tz_convert('UTC')  # Convert to UTC
    .dt.strftime('%Y-%m-%d %H:%M:%S UTC')) # Adding the UTC suffix for clarity

### Renaming the columns for clarity purposes

arb_df = arb_df.rename(columns={"question": "Question", "tags": "Tags", "yes_price": "Yes Price", "no_price":"No Price", "guaranteed_profit_percentage": "Guaranteed Profit %"})
movers_df = movers_df.rename(columns={"question": "Question", "tags": "Tags", "question_id": "Question ID", "outcome":"Outcome", "current_price": "Current Trading Price ($)", "prev_list_price": "Previous Trading Price ($)", "price_change": "Price Change ($)", "pct_change": "Price Change (%)"})
spread_df = spread_df.rename(columns={"question": "Question","outcome":"Outcome", "tags": "Tags", "ask_price": "Ask Price", "bid_price": "Bid Price", "spread":"Spread ($)", "spread_pct": "Spread (%)", "bid_shares": "# of Bid Shares (Volume)", "ask_shares": "# of Ask Shares (Volume)"})
closing_df = closing_df.rename(columns={"question": "Question", "tags": "Tags", "market_end_date":" Market End Date", "hours_until_close": "Hours until Market Close"})

### Adding each individual tab in the dashboard
tab1, tab2, tab3, tab4 = st.tabs([
    "📈 Arbitrage Opportunities", 
    "🚀 Token Movers", 
    "📏 Widest Spreads", 
    "⏳ Markets Closing Soon"
])

with tab1:
    st.subheader("🔥 Current Arbitrage Opportunities")
    st.dataframe(
        arb_df.style.format({
            'Guaranteed Profit %': '{:.2f}%', 
            'Yes Price': '{:.3f}', 
            'No Price': '{:.3f}'
        })
    )

with tab2:
    st.subheader("🏅 Top Token Movers")
    gainers = movers_df[movers_df["Price Change (%)"] > 0] \
                  .sort_values("Price Change (%)", ascending=False).head(5)
    losers = movers_df[movers_df["Price Change (%)"] < 0] \
                 .sort_values("Price Change (%)").head(5)
    movers = movers_df.loc[movers_df["Price Change ($)"].abs().sort_values(ascending=False).index].head(5)

    opt = st.selectbox("Choose mover view:", 
        ["Gainers (Proportional %)", "Losers (Proportional %)", "Absolute movers"]
    )
    if opt == "Gainers (Proportional %)":
        st.dataframe(gainers.reset_index(drop=True))
    elif opt == "Losers (Proportional %)":
        st.dataframe(losers.reset_index(drop=True))
    else:
        st.dataframe(movers.reset_index(drop=True))

with tab3:
    st.subheader("📊 Markets with Widest Spreads")
    st.dataframe(spread_df)

with tab4:
    st.subheader("⏳ Markets Closing Soon")
    closing_df["Tags"] = closing_df["Tags"].apply(lambda x: eval(x) if isinstance(x, str) else x)
    exploded = closing_df.explode("Tags")
    all_tags = sorted(exploded["Tags"].dropna().unique())
    sel = st.selectbox("Filter by tag", ["All"] + all_tags)
    if sel != "All":
        exploded = exploded[exploded["tags"] == sel]
    st.dataframe(
        exploded.sort_values("Hours until Market Close").reset_index(drop=True)
    )

### Adding the last refresh time
st.caption(f"Last data refresh: {last_refresh_time_str}")


