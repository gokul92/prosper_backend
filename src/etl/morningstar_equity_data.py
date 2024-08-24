import sys
import getopt
import os

import numpy as np
import requests
import psycopg
from psycopg.rows import dict_row
import pandas as pd
from typing import Dict, List, Union
from datetime import date, datetime, timedelta
import pandas_market_calendars as mcal
from dotenv import load_dotenv
import concurrent.futures
import time
import logging
import csv
from functools import partial
from tqdm import tqdm
import itertools
from itertools import combinations
from collections import defaultdict

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

# Constants
BASE_URL = "https://equityapi.morningstar.com/Webservice/"
EQUITY_API_PARAMS = {"responseType": "Json",
                     "Token": mstar_equity_token}

logging.basicConfig(filename='equity_data_fetcher.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# CSV logger setup
csv_log_file = 'process_times.csv'
csv_fields = [ 'timestamp', 'process', 'duration' ]


# TODO - Add depository receipt security type - ADR 'Depository-Receipt' - S1735


def log_to_csv(process, duration):
    with open(csv_log_file, 'a', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=csv_fields)
        if f.tell() == 0:
            writer.writeheader()
        writer.writerow({
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'process': process,
            'duration': duration
        })


def get_connection():
    return psycopg.connect(f'host={db_host} port={db_port} dbname={db_name} user={db_user} password={db_pass}',
                           row_factory=dict_row)


def init_db():
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS 
            """)
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


def get_symbol_guide():
    symbol_guide_url = f"http://mshxml.morningstar.com/symbolGuide?username={mstar_symbol_guide_username}&password={mstar_symbol_guide_password}&exchange=126&security=1&JSONShort"
    response = requests.get(symbol_guide_url)
    df = pd.DataFrame(response.json()[ 'quotes' ][ 'results' ])
    return df[ [ 'H1', 'S12', 'S19', 'S1735', 'S1736', 'S3059', 'S1012' ] ].to_dict(orient='records')


def get_market_price_history(security, start_date, end_date, as_of_date):
    mkt_price_params = {
        'exchangeId': security[ 'S1736' ],
        "identifierType": "Symbol",
        "identifier": security[ "H1" ],
        "startDate": start_date,
        "endDate": end_date
    }
    url = f"{BASE_URL}GlobalStockPricesService.asmx/GetEODPriceHistory"
    response = requests.get(url, params={**EQUITY_API_PARAMS, **mkt_price_params,
                                         "category": "GetEODPriceHistory"})
    if response.json()[ 'MessageInfo' ][ 'MessageCode' ] == 200:
        company_name = response.json()[ 'GeneralInfo' ][ 'CompanyName' ]
        data = response.json()[ 'EODPriceEntityList' ]
        if data:
            result = pd.DataFrame(data)
            result.drop([ 'PriceCurrencyId' ], axis=1, inplace=True)
            result[ 'Symbol' ] = security[ 'H1' ]
            result[ 'CompanyName' ] = company_name
            result.dropna(axis=0, subset='Symbol', inplace=True)
            result[ 'as_of_date' ] = as_of_date
            result.columns = [ 'date', 'open_price', 'high_price', 'low_price', 'close_price', 'volume', 'ticker',
                               'company_name', 'as_of_date' ]
            add_to_db(result, 'equities_daily_price_history')
    return None


def get_market_capitalization(security):
    market_cap_params = {
        'exchangeId': security[ 'S1736' ],
        "identifierType": "Symbol",
        "identifier": security[ "H1" ]
    }
    url = f"{BASE_URL}MarketPerformanceService.asmx/GetCurrentMarketCapitalization"
    response = requests.get(url, params={**EQUITY_API_PARAMS, **market_cap_params,
                                         "category": "GetCurrentMarketCapitalization"})
    if response.json()[ 'MessageInfo' ][ 'MessageCode' ] == 200:
        company_name = response.json()[ 'GeneralInfo' ][ 'CompanyName' ]
        data = response.json()[ 'MarketCapitalizationEntityList' ]
        if data:
            temp = data[ 0 ]
            return {
                'Symbol': security[ 'H1' ],
                'CompanyName': company_name,
                'Date': temp[ 'MarketCapDate' ],
                'MarketCap': temp[ 'MarketCap' ],
                'EnterpriseValue': temp[ 'EnterpriseValue' ],
                'SharesOutstanding': temp[ 'SharesOutstanding' ],
                'SharesDate': temp[ 'SharesDate' ]
            }
    return {}


def get_dividend_yield(security):
    div_yield_params = {
        "exchangeId": security[ 'S1736' ],
        "identifierType": "Symbol",
        "identifier": security[ "H1" ],
        "period": "Snapshot"
    }
    url = f"{BASE_URL}FinancialKeyRatiosService.asmx/GetValuationRatios"
    response = requests.get(url, params={**EQUITY_API_PARAMS, **div_yield_params,
                                         "category": "GetValuationRatios"})
    if response.json()[ 'MessageInfo' ][ 'MessageCode' ] == 200:
        company_name = response.json()[ 'GeneralInfo' ][ 'CompanyName' ]
        data = response.json()[ 'ValuationRatioEntityList' ]

        if data:
            temp = data[ 0 ]
            try:
                return {
                    'Symbol': security[ 'H1' ],
                    'CompanyName': company_name,
                    'Date': temp[ 'AsOfDate' ],
                    'DividendYield': temp[ 'DividendYield' ]
                }
            except KeyError:
                return {
                    'Symbol': security[ 'H1' ],
                    'CompanyName': company_name,
                    'Date': temp[ 'AsOfDate' ],
                    'DividendYield': 0.0
                }
    return {}


def get_equity_classification(security):
    equity_classification_params = {
        "exchangeId": security[ 'S1736' ],
        "identifier": security[ 'H1' ],
        "identifierType": "Symbol"
    }
    url = f"{BASE_URL}GlobalMasterListsService.asmx/GetCompanyFinancialAvailabilityList"
    response = requests.get(url, params={**EQUITY_API_PARAMS, **equity_classification_params,
                                         "category": "GetCompanyFinancialAvailabilityList"})
    data = response.json()[ 'CompanyFinancialAvailabilityEntityList' ]

    if data:
        temp = data[ 0 ]
        return {
            'Symbol': security[ 'H1' ],
            'CompanyName': temp[ 'CompanyName' ],
            'StartYear': temp[ 'Start' ],
            'EndYear': temp[ 'End' ],
            'SectorId': temp[ 'SectorId' ],
            'SectorName': temp[ 'SectorName' ],
            'IndustryGroupId': temp[ 'IndustryGroupId' ],
            'IndustryGroupName': temp[ 'IndustryGroupName' ],
            'IndustryId': temp[ 'IndustryId' ],
            'IndustryName': temp[ 'IndustryName' ]
        }
    return {}


def get_price_index(security, start_date, end_date, as_of_date):
    dmri_params = {
        "exchangeId": security[ 'S1736' ],
        "identifierType": "Symbol",
        "identifier": security[ 'H1' ],
        "startDate": start_date,
        "endDate": end_date
    }
    url = f"{BASE_URL}MarketPerformanceService.asmx/GetStockDMRIs"
    response = requests.get(url, params={**EQUITY_API_PARAMS, **dmri_params})
    dmri = pd.DataFrame(response.json()[ 'StockDMRIEntityList' ])

    if dmri.empty:
        return None

    dmri[ 'Symbol' ] = security[ 'H1' ]
    dmri[ 'SecurityType' ] = security[ 'S1735' ]
    dmri[ 'SecurityName' ] = security[ 'S12' ]
    dmri[ 'as_of_date' ] = as_of_date

    dmri.columns = [ 'date', 'value', 'symbol', 'security_type', 'security_name', 'as_of_date' ]

    return dmri


def process_security(security, as_of_date, latest_date='2024-07-31', db_bool=True):
    logging.info(f"Processing security: {security[ 'H1' ]}")
    latest_date_obj = datetime.strptime(latest_date, '%Y-%m-%d')
    all_data = [ ]

    while True:
        start_date_obj = latest_date_obj.replace(year=latest_date_obj.year - 2) + timedelta(days=1)
        start_date = start_date_obj.strftime('%Y-%m-%d')

        df = get_price_index(security, start_date, latest_date, as_of_date)
        if df is None or df.empty:
            break

        all_data.append(df)
        latest_date_obj = start_date_obj - timedelta(days=1)
        latest_date = latest_date_obj.strftime('%Y-%m-%d')

    if all_data:
        result = pd.concat(all_data, ignore_index=True)
        if db_bool:
            add_to_db(result, 'daily_total_return_index')
        return result
    return None


def fetch_equity_data(table_name: str, symbol: str, end_date: Union[ str, datetime ],
                      start_date: Union[ str, datetime, None ] = None,
                      as_of_date: Union[ str, datetime, None ] = None) -> Union[ Dict, List[ Dict ] ]:
    """
    Fetch equity data for a given symbol, table, and date range.

    :param table_name: The name of the table to fetch data from
    :param symbol: The ticker symbol of the equity
    :param end_date: The end date for price history and total return index queries (mandatory)
    :param start_date: The start date for price history and total return index queries (optional)
    :param as_of_date: The as-of date for the data. Can be 'latest', a string in 'YYYY-MM-DD' format, or None (which defaults to 'latest')
    :return: A dictionary or list of dictionaries containing the equity data
    """
    conn = get_connection()
    cursor = conn.cursor()

    try:
        # Validate table name to prevent SQL injection
        valid_tables = [
            'equities_dividend_yield', 'equities_industry_sector_classification',
            'equities_market_capitalization', 'equities_daily_price_history',
            'equities_total_return_index'
        ]
        if table_name not in valid_tables:
            return {"error": f"Invalid table name: {table_name}"}

        # Convert dates to datetime objects if they're strings
        if isinstance(end_date, str):
            end_date = datetime.strptime(end_date, '%Y-%m-%d').date()
        if isinstance(start_date, str):
            start_date = datetime.strptime(start_date, '%Y-%m-%d').date()

        if table_name in [ 'equities_daily_price_history', 'equities_total_return_index' ]:
            if as_of_date is None or as_of_date == 'latest':
                as_of_date_query = "MAX(as_of_date)"
            else:
                if isinstance(as_of_date, str):
                    as_of_date = datetime.strptime(as_of_date, '%Y-%m-%d').date()
                as_of_date_query = f"MAX(CASE WHEN as_of_date <= '{as_of_date}' THEN as_of_date END)"

            if start_date is None:
                query = f"""
                WITH latest_as_of_date AS (
                    SELECT {as_of_date_query} as max_date
                    FROM {table_name}
                    WHERE symbol = %s
                )
                SELECT * FROM {table_name}
                WHERE symbol = %s
                  AND date <= %s
                  AND as_of_date = (SELECT max_date FROM latest_as_of_date)
                ORDER BY date
                """
                cursor.execute(query, (symbol, symbol, end_date))
            else:
                query = f"""
                WITH latest_as_of_date AS (
                    SELECT {as_of_date_query} as max_date
                    FROM {table_name}
                    WHERE symbol = %s
                ), earliest_date AS (
                    SELECT MIN(date) as min_date
                    FROM {table_name}
                    WHERE symbol = %s AND as_of_date = (SELECT max_date FROM latest_as_of_date)
                )
                SELECT * FROM {table_name}
                WHERE symbol = %s
                  AND date BETWEEN GREATEST(%s, (SELECT min_date FROM earliest_date)) AND %s
                  AND as_of_date = (SELECT max_date FROM latest_as_of_date)
                ORDER BY date
                """
                cursor.execute(query, (symbol, symbol, symbol, start_date, end_date))
        else:
            # Existing logic for other tables
            if as_of_date is None or as_of_date == 'latest':
                query = f"""
                SELECT * FROM {table_name}
                WHERE symbol = %s
                ORDER BY as_of_date DESC
                LIMIT 1
                """
                cursor.execute(query, (symbol,))
            else:
                if isinstance(as_of_date, str):
                    as_of_date = datetime.strptime(as_of_date, '%Y-%m-%d').date()

                query = f"""
                SELECT * FROM {table_name}
                WHERE symbol = %s AND as_of_date <= %s
                ORDER BY as_of_date DESC
                LIMIT 1
                """
                cursor.execute(query, (symbol, as_of_date))

        result = cursor.fetchall()

        if not result:
            return {"error": f"No data found for symbol {symbol} in table {table_name}"}

        return [ dict(row) for row in result ]

    except Exception as e:
        return {"error": str(e)}
    finally:
        cursor.close()
        conn.close()


def fetch_equity_symbols(as_of_date):
    with get_connection() as conn:
        with conn.cursor() as cur:
            query = """
                SELECT symbol
                FROM security_master
                WHERE security_type = 'Equity' AND 
                as_of_date = %s AND
                symbol NOT IN (select symbol from security_master where security_type in ('ETF', 'CEF'))
            """
            cur.execute(query, [ as_of_date ])
            return [ row[ 'symbol' ] for row in cur.fetchall() ]


def fetch_return_data(symbols, as_of_date):
    end_date = pd.to_datetime(as_of_date)
    start_date = end_date - pd.Timedelta(days=365)

    with get_connection() as conn:
        with conn.cursor() as cur:
            query = """
            SELECT symbol, date, value
            FROM equities_total_return_index
            WHERE symbol = ANY(%s)
              AND as_of_date = %s
              AND date BETWEEN %s AND %s
            ORDER BY symbol, date DESC
            """
            cur.execute(query, [ symbols, as_of_date, start_date, end_date ])
            df = pd.DataFrame(cur.fetchall())
    return df


def process_return_data(df):
    # Filter to market trading days
    nyse = mcal.get_calendar('NYSE')
    market_days = nyse.valid_days(start_date=df[ 'date' ].min(), end_date=df[ 'date' ].max())
    market_days = [ mkt_date.date() for mkt_date in market_days ]
    df = df.loc[ df[ 'date' ].isin(market_days) & df[ 'value' ] != 0.0 ].sort_values(by=[ 'symbol', 'date' ],
                                                                                     ascending=[ True, False ])

    # Investigate why returns are null when value is never NULL
    # 1 reason - pct_change() introduces NAs in the first value in each time series
    # Calculate percentage returns
    df[ 'return' ] = df.groupby('symbol')[ 'value' ].pct_change()
    df.dropna(subset='return', inplace=True)

    return df


def efficient_symbol_lookup_dictionary(df):
    symbol_data = defaultdict(dict)
    for _, row in df.iterrows():
        symbol_data[ row[ 'symbol' ] ][ row[ 'date' ] ] = row[ 'return' ]
    return {symbol: np.array(list(dates.items()), dtype=[ ('date', 'datetime64[ns]'), ('return', 'float64') ])
            for symbol, dates in symbol_data.items()}


def calculate_correlations(symbol1, symbol2, data1, data2, durations):
    common_dates = np.intersect1d(data1[ 'date' ], data2[ 'date' ])
    common_dates.sort()
    common_dates = common_dates[ ::-1 ]  # Reverse to get most recent dates first

    results = [ ]
    for duration in durations:
        if len(common_dates) < duration:
            results.append((symbol1, symbol2, None, duration))
            continue

        dates_to_use = common_dates[ :duration ]
        returns1 = data1[ np.isin(data1[ 'date' ], dates_to_use) ][ 'return' ]
        returns2 = data2[ np.isin(data2[ 'date' ], dates_to_use) ][ 'return' ]

        if (len(np.unique(returns1)) < 3 or len(np.unique(returns2)) < 3 or
                np.all(returns1 == 0) or np.all(returns2 == 0) or
                np.std(returns1) == 0 or np.std(returns2) == 0 or
                np.array_equal(returns1, returns2)):
            correlation = None
        else:
            correlation = np.corrcoef(returns1, returns2)[ 0, 1 ]

        results.append((symbol1, symbol2, correlation, duration))

    return results


def insert_correlations_batch(batch, conn, cur):
    query = """
        INSERT INTO equities_correlation (as_of_date, symbol, symbol2, correlation, duration)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (as_of_date, symbol, symbol2, duration) DO UPDATE
        SET correlation = EXCLUDED.correlation
    """
    try:
        cur.executemany(query, batch)
        conn.commit()
        return len(batch)
    except Exception as e:
        conn.rollback()
        print(f"Error inserting batch: {str(e)}")
        return 0


def calculate_equity_correlations(as_of_date):
    symbols = fetch_equity_symbols("2024-08-10")  # change later
    returns_data = fetch_return_data(symbols, as_of_date)
    processed_data = process_return_data(returns_data)
    symbols = processed_data[ 'symbol' ].unique()
    print("processed returns data")

    durations = [ 21, 63, 105, 126, 189, 250 ]
    num_durations = len(durations)

    total_pairs = len(symbols) * (len(symbols) - 1) // 2
    print(f"Total number of pairs to process: {total_pairs}")

    # Pre-allocate the list for all correlations
    all_correlations = [ [ None ] * 5 for _ in range(total_pairs * num_durations) ]

    chunk_size = 10000  # Adjust this based on your available memory
    symbol_pairs = combinations(symbols, 2)
    symbol_data = efficient_symbol_lookup_dictionary(processed_data)

    with concurrent.futures.ProcessPoolExecutor() as executor:
        with tqdm(total=total_pairs, desc="Calculating correlations") as pbar:
            current_index = 0
            while True:
                chunk = list(itertools.islice(symbol_pairs, chunk_size))
                if not chunk:
                    break

                args_list = [ (s1, s2, symbol_data[ s1 ], symbol_data[ s2 ], durations) for s1, s2 in chunk ]
                results = list(executor.map(calculate_correlations, *zip(*args_list)))

                # Flatten results and prepare data for insertion
                for result in results:
                    for corr in result:
                        all_correlations[ current_index ] = [
                            as_of_date,
                            str(corr[ 0 ]),
                            str(corr[ 1 ]),
                            float(corr[ 2 ]) if corr[ 2 ] is not None else None,
                            int(corr[ 3 ])
                        ]
                        current_index += 1

                pbar.update(len(chunk))

    print(f"Correlation calculation completed for {total_pairs} pairs.")
    print("Starting database insertion...")

    # Insert results into database in batches
    batch_size = 1000000
    total_correlations = len(all_correlations)
    inserted = 0

    with get_connection() as conn:
        with conn.cursor() as cur:
            for i in range(0, total_correlations, batch_size):
                batch = all_correlations[ i:i + batch_size ]
                inserted += insert_correlations_batch(batch, conn, cur)
                print(f"Inserted {inserted}/{total_correlations} correlations")

    print(f"Insertion completed. Total inserted: {inserted}/{total_correlations}")


def main(eq_perf_bool, eq_ind_bool, eq_yield_bool, eq_price_hist_bool, mcap_bool, corr_bool, num_workers, as_of_date,
         start_date, end_date):
    # TODO - Fix init_db and check where symbol guide needs to be called
    # TODO - replace calls to symbol_guide_mstar with calls to the database security master
    # init_db() - FIX init_db() function and uncomment!
    if eq_ind_bool:
        symbol_guide_mstar = get_symbol_guide()
        start_time_equity_classification = time.time()
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
            equity_classification_results = list(executor.map(get_equity_classification, symbol_guide_mstar))
        equity_classification_df = pd.DataFrame(equity_classification_results).dropna(subset='Symbol')
        equity_classification_df[ 'as_of_date' ] = as_of_date
        equity_classification_df.columns = [ 'symbol', 'security_name', 'start_year', 'end_year', 'sector_id',
                                             'sector_name', 'industry_group_id', 'industry_group_name',
                                             'industry_id', 'industry_name', 'as_of_date' ]
        add_to_db(equity_classification_df, 'daily_industry_sector_classification')
        end_time_equity_classification = time.time()
        duration = end_time_equity_classification - start_time_equity_classification
        log_to_csv('Equity Classification', duration)
        logging.info(f"Equity classification completed in {duration:.2f} seconds")

    if eq_yield_bool:
        symbol_guide_mstar = get_symbol_guide()
        start_time_dividend_yield = time.time()
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
            dividend_yield_results = list(executor.map(get_dividend_yield, symbol_guide_mstar))
        dividend_yield_results = [ d for d in dividend_yield_results if d != {} ]
        dividend_yield_df = pd.DataFrame(dividend_yield_results).dropna(subset='Symbol')
        dividend_yield_df[ 'as_of_date' ] = as_of_date
        dividend_yield_df.columns = [ 'symbol', 'company_name', 'date', 'dividend_yield', 'as_of_date' ]
        add_to_db(dividend_yield_df, 'equities_dividend_yield')
        end_time_dividend_yield = time.time()
        duration = end_time_dividend_yield - start_time_dividend_yield
        log_to_csv('Yield Classification', duration)
        logging.info(f"Yield classification completed in {duration:.2f} seconds")

    if eq_price_hist_bool and start_date is not None and end_date is not None:
        symbol_guide_mstar = get_symbol_guide()
        start_time_price_hist = time.time()
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
            partial_func = partial(get_market_price_history, start_date=start_date, end_date=end_date,
                                   as_of_date=as_of_date)
            executor.map(partial_func, symbol_guide_mstar)
        end_time_price_hist = time.time()
        duration = end_time_price_hist - start_time_price_hist
        log_to_csv('Price History', duration)
        logging.info(f"Price history completed in {duration:.2f} seconds")

    if mcap_bool:
        symbol_guide_mstar = get_symbol_guide()
        start_time_mcap = time.time()
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
            mcap_result = list(executor.map(get_market_capitalization, symbol_guide_mstar))
        mcap_result = [ d for d in mcap_result if d != {} ]
        mcap_df = pd.DataFrame(mcap_result).dropna(subset='Symbol')
        mcap_df[ 'as_of_date' ] = as_of_date
        mcap_df.columns = [ 'symbol', 'company_name', 'date', 'market_cap', 'enterprise_value',
                            'shares_outstanding', 'shares_date', 'as_of_date' ]
        add_to_db(mcap_df, 'equities_market_capitalization')
        end_time_mcap = time.time()
        duration = end_time_mcap - start_time_mcap
        log_to_csv('Market Capitalization', duration)
        logging.info(f"Market cap completed in {duration:.2f} seconds")

    if eq_perf_bool:
        symbol_guide_mstar = get_symbol_guide()
        start_time_equity_performance = time.time()
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
            partial_func = partial(process_security, as_of_date)
            executor.map(partial_func, symbol_guide_mstar)
        end_time_equity_performance = time.time()
        duration = end_time_equity_performance - start_time_equity_performance
        log_to_csv('Equity Performance', duration)
        logging.info(f"Equity performance completed in {duration:.2f} seconds")

    if corr_bool:
        calculate_equity_correlations(as_of_date)


if __name__ == "__main__":
    opts, args = getopt.getopt(sys.argv[ 1: ], "piyhmcfs:w:d:e:a:",
                               [ "performance", "industry", "yield", "history", "mcap", "corr", "workers=", "date=",
                                 "end_date=", "days=" ])
    perf = False
    industry = False
    yld = False
    history = False
    mcap = False
    corr = False
    fetch = False
    symbol = None
    n_workers = 25
    as_of_date = date.today().strftime('%Y-%m-%d')
    start_date = None
    end_date = None
    num_days = None

    for opt, arg in opts:
        if opt in ("-p", "--performance"):
            perf = True
        elif opt in ("-i", "--industry"):
            industry = True
        elif opt in ("-y", "--yield"):
            yld = True
        elif opt in ("-h", "--history"):
            history = True
        elif opt in ("-m", "--mcap"):
            mcap = True
        elif opt in ("-c", "--corr"):
            corr = True
        elif opt in ("-f", "--fetch"):
            fetch = True
        elif opt in ("-w", "--workers"):
            n_workers = int(arg)
        elif opt in ("-s", "--symbol"):
            symbol = arg
        elif opt in ("-d", "--date"):
            as_of_date = arg
        elif opt in ("-e", "--end_date"):
            end_date = arg
        elif opt in ("-a", "--days"):
            num_days = int(arg)
            if end_date is not None:
                end_date_obj = datetime.strptime(end_date, "%Y-%m-%d")
                start_date = (end_date_obj - timedelta(days=num_days)).strftime("%Y-%m-%d")

    main(perf, industry, yld, history, mcap, corr, n_workers, as_of_date, start_date, end_date)
