import sys
import getopt
import os
import requests
import psycopg
from psycopg.rows import dict_row
import pandas as pd
from datetime import date, datetime, timedelta
from dotenv import load_dotenv
import concurrent.futures
import time
import logging
import csv
from functools import partial

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


# TODO - Add latest share price, market cap/AUM for equities
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
            CREATE INDEX IF NOT EXISTS idx_equities_daily_price_history_ticker ON equities_daily_price_history(ticker);
            CREATE INDEX IF NOT EXISTS idx_equities_daily_price_history_as_of_date ON equities_daily_price_history(as_of_date);
            CREATE INDEX IF NOT EXISTS idx_equities_daily_price_history_created_at ON equities_daily_price_history(created_at);

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
    return df[ [ 'H1', 'S12', 'S19', 'S1735', 'S1736', 'S3059' ] ].to_dict(orient='records')


def get_market_price_history(security, start_date, end_date, as_of_date):
    mkt_price_params = {
        'exchangeId': security[ 'S1736' ],
        "identifierType": "Symbol",
        "identifier": security[ "H1" ],
        "startDate": start_date,
        "endDate": end_date
    }
    # print(start_date, end_date, as_of_date)
    url = f"{BASE_URL}GlobalStockPricesService.asmx/GetEODPriceHistory"
    response = requests.get(url, params={**EQUITY_API_PARAMS, **mkt_price_params,
                                         "category": "GetEODPriceHistory"})
    # response.raise_for_status()  # This will raise an exception for HTTP errors
    # logging.info(f"Received response for {security[ 'H1' ]}")
    # print(f"Response for {security[ 'H1' ]}:", response.text)  # Print raw response
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
    pass


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


def fetch_equity_classification(symbols, as_of_date):
    symbols_tuple = tuple(symbols) if isinstance(symbols, list) else (symbols,)
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT * FROM daily_industry_sector_classification
                WHERE symbol = %s AND as_of_date = %s
                ORDER BY as_of_date DESC
            """, (symbols_tuple, as_of_date))
            return cur.fetchone()


def fetch_equity_performance(symbols, start_date, end_date, as_of_date):
    symbols_tuple = tuple(symbols) if isinstance(symbols, list) else (symbols,)
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT * FROM daily_total_return_index
                WHERE symbol IN %s AND date BETWEEN %s AND %s AND as_of_date = %s
                ORDER BY symbol, date
            """, (symbols_tuple, start_date, end_date, as_of_date))
            return cur.fetchall()


def main(eq_perf_bool, eq_ind_bool, eq_yield_bool, eq_price_hist_bool, num_workers, as_of_date, start_date, end_date):
    init_db()
    symbol_guide_mstar = get_symbol_guide()
    if eq_ind_bool:
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
        # print("entered price history")
        start_time_price_hist = time.time()
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
            # executor.map(get_market_price_history, symbol_guide_mstar, start_date, end_date, as_of_date)
            partial_func = partial(get_market_price_history, start_date=start_date, end_date=end_date,
                                   as_of_date=as_of_date)
            executor.map(partial_func, symbol_guide_mstar)
        end_time_price_hist = time.time()
        duration = end_time_price_hist - start_time_price_hist
        log_to_csv('Price History', duration)
        logging.info(f"Price history completed in {duration:.2f} seconds")

    if eq_perf_bool:
        start_time_equity_performance = time.time()
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
            executor.map(process_security, symbol_guide_mstar, as_of_date)
        end_time_equity_performance = time.time()
        duration = end_time_equity_performance - start_time_equity_performance
        log_to_csv('Equity Performance', duration)
        logging.info(f"Equity performance completed in {duration:.2f} seconds")


if __name__ == "__main__":
    opts, args = getopt.getopt(sys.argv[ 1: ], "piyhw:d:e:a:",
                               [ "performance", "industry", "yield", "history", "workers=", "date=", "end_date=",
                                 "days=" ])
    perf = False
    industry = False
    yld = False
    history = False
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
        elif opt in ("-w", "--workers"):
            n_workers = int(arg)
        elif opt in ("-d", "--date"):
            as_of_date = arg
        elif opt in ("-e", "--end_date"):
            end_date = arg
        elif opt in ("-a", "--days"):
            num_days = int(arg)
            if end_date is not None:
                end_date_obj = datetime.strptime(end_date, "%Y-%m-%d")
                start_date = (end_date_obj - timedelta(days=num_days)).strftime("%Y-%m-%d")

    main(perf, industry, yld, history, n_workers, as_of_date, start_date, end_date)
