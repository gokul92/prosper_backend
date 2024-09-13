import os
import psycopg
import logging
from dotenv import load_dotenv
from psycopg.rows import dict_row

load_dotenv()

# Database connection parameters
db_host = os.getenv('DB_HOST')
db_user = os.getenv('DB_USERNAME')
db_pass = os.getenv('DB_PASSWORD')
db_name = os.getenv('DB_NAME')
db_port = os.getenv('DB_PORT')
mstar_equity_token = os.getenv('MSTAR_EQUITY_TOKEN')
mstar_symbol_guide_username = os.getenv('MSTAR_SYMBOL_GUIDE_USERNAME')
mstar_symbol_guide_password = os.getenv('MSTAR_SYMBOL_GUIDE_PASSWORD')


def get_connection():
    return psycopg.connect(f'host={db_host} port={db_port} dbname={db_name} user={db_user} password={db_pass}',
                           row_factory=dict_row)


def init_db():
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS daily_total_return_index (
                    as_of_date DATE DEFAULT CURRENT_DATE,
                    symbol TEXT NOT NULL,
                    value DOUBLE PRECISION,
                    date DATE NOT NULL,
                    created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
                    security_type TEXT NOT NULL,
                    security_name TEXT NOT NULL
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS daily_industry_sector_classification (
                    as_of_date DATE DEFAULT CURRENT_DATE,
                    symbol TEXT NOT NULL,
                    security_name TEXT NOT NULL,
                    start_year INT NOT NULL,
                    end_year INT,
                    sector_id TEXT NOT NULL,
                    sector_name TEXT NOT NULL,
                    industry_group_id TEXT NOT NULL,
                    industry_group_name TEXT NOT NULL,
                    industry_id TEXT NOT NULL,
                    industry_name TEXT NOT NULL,
                    created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS equities_dividend_yield (
                    symbol TEXT NOT NULL,
                    company_name TEXT NOT NULL,
                    date DATE NOT NULL,
                    as_of_date DATE NOT NULL,
                    dividend_yield DOUBLE PRECISION,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
            )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS equities_daily_price_history (
                date DATE NOT NULL,
                open_price DOUBLE PRECISION,
                high_price DOUBLE PRECISION,
                low_price DOUBLE PRECISION,
                close_price DOUBLE PRECISION,
                volume DOUBLE PRECISION,
                ticker TEXT NOT NULL,
                company_name TEXT,
                as_of_date DATE NOT NULL,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                CONSTRAINT pk_equities_daily_price_history PRIMARY KEY (date, ticker, as_of_date)
            );

            -- Create indexes for faster querying
            CREATE INDEX IF NOT EXISTS idx_equities_daily_price_history_ticker ON equities_price_history(ticker);
            CREATE INDEX IF NOT EXISTS idx_equities_daily_price_history_as_of_date ON equities_price_history(as_of_date);
            CREATE INDEX IF NOT EXISTS idx_equities_daily_price_history_created_at ON equities_price_history(created_at);

            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS equities_market_capitalization (
                symbol TEXT NOT NULL,
                company_name TEXT,
                date DATE NOT NULL,
                market_cap DOUBLE PRECISION,
                enterprise_value DOUBLE PRECISION,
                shares_outstanding DOUBLE PRECISION,
                shares_date DATE,
                as_of_date DATE NOT NULL,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                CONSTRAINT pk_equities_market_capitalization PRIMARY KEY (symbol, as_of_date, date)
            );

            -- Create indexes for faster querying
            CREATE INDEX IF NOT EXISTS idx_equities_market_cap_symbol ON equities_market_capitalization(symbol);
            CREATE INDEX IF NOT EXISTS idx_equities_market_cap_date ON equities_market_capitalization(date);
            CREATE INDEX IF NOT EXISTS idx_equities_market_cap_as_of_date ON equities_market_capitalization(as_of_date);
            """)
            conn.commit()


def add_to_db(df, table_name):
    with get_connection() as conn:
        with conn.cursor() as cur:
            columns = ', '.join(df.columns)
            values = ', '.join([ '%s' for _ in df.columns ])
            insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({values})"
            cur.executemany(insert_query, df.values.tolist())
            conn.commit()
    logging.info(f"Added {len(df)} rows to {table_name}")
