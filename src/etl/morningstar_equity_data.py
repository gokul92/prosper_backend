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
    SYMBOL_GUIDE_URL = f"http://mshxml.morningstar.com/symbolGuide?username={mstar_symbol_guide_username}&password={mstar_symbol_guide_password}&exchange=126&security=1&JSONShort"
    response = requests.get(SYMBOL_GUIDE_URL)
    df = pd.DataFrame(response.json()[ 'quotes' ][ 'results' ])
    return df[ [ 'H1', 'S12', 'S19', 'S1735', 'S1736', 'S3059' ] ].to_dict(orient='records')


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

    if response.json()['MessageInfo']['MessageCode'] == 200:
        company_name = response.json()[ 'GeneralInfo' ]['CompanyName']
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


def main(eq_perf_bool, eq_ind_bool, eq_yield_bool, as_of_date, num_workers=25):
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
            dividend_yield_results = [d for d in dividend_yield_results if d != {}]
            dividend_yield_df = pd.DataFrame(dividend_yield_results).dropna(subset='Symbol')
            dividend_yield_df[ 'as_of_date' ] = as_of_date
            dividend_yield_df.columns = [ 'symbol', 'company_name', 'date', 'dividend_yield', 'as_of_date' ]
            add_to_db(dividend_yield_df, 'equities_dividend_yield')
        end_time_dividend_yield = time.time()
        duration = end_time_dividend_yield - start_time_dividend_yield
        log_to_csv('Yield Classification', duration)
        logging.info(f"Yield classification completed in {duration:.2f} seconds")

    if eq_perf_bool:
        start_time_equity_performance = time.time()
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
            list(executor.map(process_security, symbol_guide_mstar, as_of_date))
        end_time_equity_performance = time.time()
        duration = end_time_equity_performance - start_time_equity_performance
        log_to_csv('Equity Performance', duration)
        logging.info(f"Equity performance completed in {duration:.2f} seconds")


if __name__ == "__main__":
    opts, args = getopt.getopt(sys.argv[ 1: ], "piydw:", [ "performance", "industry", "yield", "date=", "workers=" ])
    eqpb = False
    eqib = False
    eqyb = False
    dt = date.today().strftime('%Y-%m-%d')
    nw = 25

    for opt, arg in opts:
        if opt in ("-p", "--performance"):
            eqpb = True
        elif opt in ("-i", "--industry"):
            eqcb = True
        elif opt in ("-y", "--yield"):
            eqyb = True
        elif opt in ("-d", "--date"):
            dt = arg
        elif opt in ("-w", "--workers"):
            nw = int(arg)

    main(eqpb, eqib, eqyb, nw)
