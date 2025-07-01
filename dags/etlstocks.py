from airflow.decorators import dag, task
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta, date

# DAG-level constants
POSTGRES_CONN_ID = 'postgres_default'
API_CONN_ID = 'alphavantage_api'
STOCK_SYMBOL = 'AAPL'
API_KEY = 'YOUR_API_KEY'  # Consider using Airflow Variables or Secrets
FUNCTION = 'TIME_SERIES_DAILY'


@dag(
    dag_id='stocks_etl_pipeline',
    schedule='@daily',  # ✅ new style
    start_date=datetime(2024, 1, 1),  # static start date required
    catchup=False,
    tags=["stocks", "ETL"]
)
def etl_pipeline():

    @task()
    def extract_stock_data():
        """Extract daily stock prices from Alpha Vantage using Airflow HTTP Connection."""
        http_hook = HttpHook(http_conn_id=API_CONN_ID, method='GET')
        endpoint = f'/query?function={FUNCTION}&symbol={STOCK_SYMBOL}&apikey={API_KEY}&outputsize=compact&datatype=json'
        response = http_hook.run(endpoint)

        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Failed to fetch stock data: {response.status_code}")

    @task()
    def transform_stock_data(stock_data):
        """Transform extracted stock data to select only today's info."""
        time_series = stock_data.get("Time Series (Daily)", {})
        today = date.today().isoformat()

        if today not in time_series:
            raise Exception(f"No data available for today: {today}")

        daily_data = time_series[today]
        return {
            'date': today,
            'symbol': stock_data.get("Meta Data", {}).get("2. Symbol", "UNKNOWN"),
            'open': float(daily_data["1. open"]),
            'high': float(daily_data["2. high"]),
            'low': float(daily_data["3. low"]),
            'close': float(daily_data["4. close"]),
            'volume': int(daily_data["5. volume"])
        }

    @task()
    def load_stock_data(transformed_data):
        """Load transformed stock data into PostgreSQL."""
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        # Create table if it doesn't exist
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS stock_data (
            date DATE,
            symbol TEXT,
            open FLOAT,
            high FLOAT,
            low FLOAT,
            close FLOAT,
            volume BIGINT,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)

        cursor.execute("""
        INSERT INTO stock_data (date, symbol, open, high, low, close, volume)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            transformed_data['date'],
            transformed_data['symbol'],
            transformed_data['open'],
            transformed_data['high'],
            transformed_data['low'],
            transformed_data['close'],
            transformed_data['volume']
        ))

        conn.commit()
        cursor.close()

    # Orchestration
    raw_data = extract_stock_data()
    transformed = transform_stock_data(raw_data)
    load_stock_data(transformed)


dag = etl_pipeline()
