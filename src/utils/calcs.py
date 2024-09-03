from typing import List, Dict, Tuple
import psycopg
from psycopg.rows import dict_row
import os
from dotenv import load_dotenv
import pandas as pd
import numpy as np
import pandas_market_calendars as mcal

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

def fetch_account_data(account_id: str, as_of_date: str) -> Dict:
    query = """
    WITH latest_holdings AS (
        SELECT account_id, MAX(as_of_date) as max_date
        FROM account_holdings
        WHERE account_id = %s AND as_of_date <= %s
        GROUP BY account_id
    )
    SELECT ah.symbol, ah.quantity, ah.adjusted_cost_basis, ah.as_of_date,
           sm.security_type, sm.mstar_id,
           CASE
               WHEN sm.security_type = 'Equity' THEN eph.close_price
               ELSE fmp.day_end_market_price
           END as price,
           hrs.annualized_return, hrs.annualized_volatility
    FROM account_holdings ah
    JOIN latest_holdings lh ON ah.account_id = lh.account_id AND ah.as_of_date = lh.max_date
    LEFT JOIN LATERAL (
        SELECT symbol, security_type, mstar_id
        FROM security_master
        WHERE symbol = ah.symbol AND as_of_date <= %s
        ORDER BY as_of_date DESC
        LIMIT 1
    ) sm ON true
    LEFT JOIN LATERAL (
        SELECT ticker, close_price
        FROM equities_price_history
        WHERE ticker = ah.symbol AND date <= %s AND as_of_date <= %s
        ORDER BY date DESC, as_of_date DESC
        LIMIT 1
    ) eph ON sm.security_type = 'Equity'
    LEFT JOIN LATERAL (
        SELECT ticker, day_end_market_price
        FROM funds_market_price_and_capitalization
        WHERE ticker = ah.symbol AND date <= %s AND as_of_date <= %s
        ORDER BY date DESC, as_of_date DESC
        LIMIT 1
    ) fmp ON sm.security_type IN ('OEF', 'CEF', 'ETF', 'MMF')
    LEFT JOIN LATERAL (
        SELECT symbol, annualized_return, annualized_volatility
        FROM historical_return_statistics
        WHERE symbol = ah.symbol AND as_of_date <= %s
        ORDER BY as_of_date DESC
        LIMIT 1
    ) hrs ON true
    WHERE ah.account_id = %s
    """
    
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (account_id, as_of_date, as_of_date, as_of_date, as_of_date, as_of_date, as_of_date, as_of_date, account_id))
            holdings = cur.fetchall()
    
    return holdings

def fetch_total_return_indices(symbols: List[str], symbol_types: Dict[str, str], as_of_date: str) -> Dict[str, pd.DataFrame]:
    equity_symbols = [symbol for symbol, type in symbol_types.items() if type == 'Equity']
    fund_symbols = [symbol for symbol, type in symbol_types.items() if type in ["OEF", "CEF", "ETF", "MMF"]]
    
    total_return_indices = {}
    
    if equity_symbols:
        equity_query = """
        WITH latest_as_of_date AS (
            SELECT symbol, MAX(as_of_date) as as_of_date
            FROM equities_total_return_index
            WHERE symbol = ANY(%s) AND as_of_date <= %s
            GROUP BY symbol
        )
        SELECT e.symbol, e.date, e.value
        FROM equities_total_return_index e
        JOIN latest_as_of_date lad ON e.symbol = lad.symbol AND e.as_of_date = lad.as_of_date
        WHERE e.date <= %s
        ORDER BY e.symbol, e.date
        """
        
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(equity_query, (equity_symbols, as_of_date, as_of_date))
                equity_results = cur.fetchall()
        
        for row in equity_results:
            symbol = row['symbol']
            if symbol not in total_return_indices:
                total_return_indices[symbol] = []
            total_return_indices[symbol].append({'date': row['date'], 'value': row['value']})
    
    if fund_symbols:
        fund_query = """
        WITH latest_as_of_date AS (
            SELECT symbol, MAX(as_of_date) as as_of_date
            FROM funds_total_return_index
            WHERE symbol = ANY(%s) AND as_of_date <= %s
            GROUP BY symbol
        )
        SELECT f.symbol, f.date, f.value
        FROM funds_total_return_index f
        JOIN latest_as_of_date lad ON f.symbol = lad.symbol AND f.as_of_date = lad.as_of_date
        WHERE f.date <= %s
        ORDER BY f.symbol, f.date
        """
        
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(fund_query, (fund_symbols, as_of_date, as_of_date))
                fund_results = cur.fetchall()
        
        for row in fund_results:
            symbol = row['symbol']
            if symbol not in total_return_indices:
                total_return_indices[symbol] = []
            total_return_indices[symbol].append({'date': row['date'], 'value': row['value']})
    
    # Convert to DataFrame and filter for trading days
    nyse = mcal.get_calendar('NYSE')
    for symbol, data in total_return_indices.items():
        df = pd.DataFrame(data)
        trading_days = nyse.valid_days(start_date=df['date'].min(), end_date=df['date'].max())
        trading_days = trading_days.date  # Extract date from datetime
        df = df.loc[df['date'].isin(trading_days)]
        total_return_indices[symbol] = df
    
    return total_return_indices

def process_return_data(total_return_indices: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
    return_data = {}
    
    for symbol, df in total_return_indices.items():
        df = df.sort_values(by='date', ascending=True)
        df['return'] = df['value'].pct_change()
        df = df.dropna(subset=['return'])
        
        # Remove leading rows with 0 return values
        first_non_zero_index = df['return'].ne(0).idxmax()
        df = df.loc[first_non_zero_index:]
        
        return_data[symbol] = df
    
    return return_data

def calculate_symbol_weights(holdings: List[Dict]) -> Dict[str, Dict[str, float]]:
    total_value = sum(holding['quantity'] * holding['price'] for holding in holdings)
    
    weights = {}
    for holding in holdings:
        symbol = holding['symbol']
        weight = (holding['quantity'] * holding['price']) / total_value if total_value > 0 else 0
        weights[symbol] = {
            'weight': weight,
            'return': holding['annualized_return'],
            'volatility': holding['annualized_volatility']
        }
    
    return weights

def calculate_portfolio_return(weights: Dict[str, Dict[str, float]]) -> float:
    return sum(data['weight'] * data['return'] for data in weights.values())

def calculate_covariances(return_data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    # Find the common date range
    common_dates = list(set.intersection(*[set(data.index) for data in return_data.values()]))
    common_dates.sort()  # Ensure dates are in order
    
    # Filter returns to only include common dates
    filtered_returns = {symbol: data.loc[common_dates]['return'] for symbol, data in return_data.items()}
    
    returns = pd.DataFrame(filtered_returns)
    
    # Calculate the covariance matrix
    cov_matrix = returns.cov()
    
    # Annualize the covariance matrix (assuming daily returns)
    trading_days_per_year = 252  # Adjust this if using a different frequency
    cov_matrix_annual = cov_matrix * trading_days_per_year
    
    return cov_matrix_annual

def calculate_portfolio_volatility(weights: Dict[str, Dict[str, float]], covariance_matrix: pd.DataFrame) -> float:
    if len(weights) == 1:
        # If there's only one holding, return its volatility directly
        return list(weights.values())[0]['volatility']
    
    # For multiple holdings, proceed with the matrix calculation
    weight_array = np.array([weights[symbol]['weight'] for symbol in covariance_matrix.index])
    volatility_array = np.array([weights[symbol]['volatility'] for symbol in covariance_matrix.index])
    
    # Construct a correlation matrix from the covariance matrix
    std_devs = np.sqrt(np.diag(covariance_matrix))
    corr_matrix = covariance_matrix / np.outer(std_devs, std_devs)
    
    # Construct the covariance matrix using annualized volatilities and correlation matrix
    annualized_cov_matrix = np.outer(volatility_array, volatility_array) * corr_matrix
    
    portfolio_variance = weight_array.T @ annualized_cov_matrix @ weight_array
    return np.sqrt(portfolio_variance)

def portfolio_statistics(account_id: str, as_of_date: str) -> Dict[str, float]:
    # Fetch all account data in one query
    holdings = fetch_account_data(account_id, as_of_date)
    
    # Calculate symbol weights and get individual returns and volatilities
    weights = calculate_symbol_weights(holdings)
    
    # Calculate portfolio return
    portfolio_return = calculate_portfolio_return(weights)
    
    # Fetch total return indices and process return data
    symbol_types = {holding['symbol']: holding['security_type'] for holding in holdings}
    total_return_indices = fetch_total_return_indices([h['symbol'] for h in holdings], symbol_types, as_of_date)
    return_data = process_return_data(total_return_indices)
    
    # Calculate covariances
    covariance_matrix = calculate_covariances(return_data)
    
    # Calculate portfolio volatility
    portfolio_volatility = calculate_portfolio_volatility(weights, covariance_matrix)
    
    return {
        'return': portfolio_return,
        'volatility': portfolio_volatility,
        'weights': {symbol: data['weight'] for symbol, data in weights.items()}
    }

# Example usage:
stats = portfolio_statistics('f828a152-4a1d-4fbc-bf79-4ec0acdb2aca', '2024-08-31')
print(f"Portfolio Return: {stats['return']:.4f}")
print(f"Portfolio Volatility: {stats['volatility']:.4f}")
print("Weights:", stats['weights'])