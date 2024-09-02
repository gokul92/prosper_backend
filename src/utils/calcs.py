from typing import Optional, List, Dict
import psycopg
from psycopg.rows import dict_row
import os
from dotenv import load_dotenv
import pandas as pd
import numpy as np
import pandas_market_calendars as mcal
from concurrent.futures import ThreadPoolExecutor
from itertools import combinations

load_dotenv()

# Database connection parameters
db_host = os.getenv('DB_HOST')
db_user = os.getenv('DB_USERNAME')
db_pass = os.getenv('DB_PASSWORD')
db_name = os.getenv('DB_NAME')
db_port = os.getenv('DB_PORT')

def get_connection():
    return psycopg.connect(f'host={db_host} port={db_port} dbname={db_name} user={db_user} password={db_pass}',
                           row_factory=dict_row)

def fetch_symbol_types(symbols: List[str], as_of_date: str) -> Dict[str, str]:
    query = """
    WITH latest_as_of_date AS (
        SELECT symbol, MAX(as_of_date) as as_of_date
        FROM security_master
        WHERE symbol = ANY(%s) AND as_of_date <= %s
        GROUP BY symbol
    )
    SELECT sm.symbol, sm.security_type
    FROM security_master sm
    JOIN latest_as_of_date lad
    ON sm.symbol = lad.symbol AND sm.as_of_date = lad.as_of_date
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (symbols, as_of_date))
            rows = cur.fetchall()
    
    symbol_types = {}
    for row in rows:
        symbol = row['symbol']
        security_type = row['security_type']
        if symbol in symbol_types:
            if security_type != 'Equity':
                symbol_types[symbol] = security_type
        else:
            symbol_types[symbol] = security_type
    
    return symbol_types

def fetch_total_return_indices(symbols: List[str], symbol_types: Dict[str, str], as_of_date: str) -> Dict[str, pd.DataFrame]:
    total_return_indices = {}
    nyse = mcal.get_calendar('NYSE')
    
    for symbol in symbols:
        symbol_type = symbol_types.get(symbol)
        if not symbol_type:
            continue
        
        if symbol_type == 'Equity':
            query = """
            WITH latest_as_of_date AS (
                SELECT MAX(as_of_date) as as_of_date
                FROM equities_total_return_index
                WHERE symbol = %s AND as_of_date <= %s
            )
            SELECT date, value
            FROM equities_total_return_index
            WHERE symbol = %s 
              AND as_of_date = (SELECT as_of_date FROM latest_as_of_date)
              AND date <= %s
            ORDER BY date
            """
        else:
            query = """
            WITH latest_as_of_date AS (
                SELECT MAX(as_of_date) as as_of_date
                FROM funds_total_return_index
                WHERE mstar_id = %s AND as_of_date <= %s
            )
            SELECT date, value
            FROM funds_total_return_index
            WHERE mstar_id = %s 
              AND as_of_date = (SELECT as_of_date FROM latest_as_of_date)
              AND date <= %s
            ORDER BY date
            """
        
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (symbol, as_of_date, symbol, as_of_date))
                result = cur.fetchall()
        
        if not result:
            print(f"No data found for symbol: {symbol}")
            continue
        
        df = pd.DataFrame(result, columns=['date', 'value'])
        
        # Ensure dates are in the same format
        df['date'] = pd.to_datetime(df['date']).dt.date
        trading_days = nyse.valid_days(start_date=df['date'].min(), end_date=df['date'].max()).date
        
        df = df.loc[df['date'].isin(trading_days)]
        
        total_return_indices[symbol] = df
    
    return total_return_indices

def process_return_data(total_return_indices: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
    return_data = {}
    
    for symbol, df in total_return_indices.items():
        df['return'] = df.sort_values(by='date', ascending=True)['value'].pct_change()
        df.dropna(subset=['return'], inplace=True)
        
        # Remove leading rows with 0 return values
        first_non_zero_index = df['return'].ne(0).idxmax()
        df = df.loc[first_non_zero_index:]
        
        return_data[symbol] = df
    
    return return_data

def calculate_covariances(symbols: List[str], as_of_date: str) -> Dict[tuple, float]:
    symbol_types = fetch_symbol_types(symbols, as_of_date)
    total_return_indices = fetch_total_return_indices(symbols, symbol_types, as_of_date)
    
    return_data = process_return_data(total_return_indices)
    
    covariances = {}
    for (symbol1, df1), (symbol2, df2) in combinations(return_data.items(), 2):
        common_dates = np.intersect1d(df1['date'], df2['date'])
        if len(common_dates) < 2:
            continue
        
        returns1 = df1[df1['date'].isin(common_dates)]['return']
        returns2 = df2[df2['date'].isin(common_dates)]['return']
        
        covariance = np.cov(returns1, returns2)[0, 1]
        covariances[(symbol1, symbol2)] = covariance
    
    return covariances