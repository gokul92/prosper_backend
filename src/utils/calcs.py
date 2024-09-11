from typing import List, Dict, Tuple, Union
import psycopg
from psycopg.rows import dict_row
import os
from dotenv import load_dotenv
import pandas as pd
import numpy as np
import pandas_market_calendars as mcal
import json
import datetime
from src.domain_logic.optimization import PortfolioOptimizer
from src.domain_logic.simulation import simulate_balance_paths

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
        
        #TODO - This is not efficient. Need to improve performance.
        for row in equity_results:
            symbol = row['symbol']
            if symbol not in total_return_indices:
                total_return_indices[symbol] = []
            total_return_indices[symbol].append({'date': row['date'], 'value': row['value']})
    
    if fund_symbols:
        fund_query = """
        WITH security_master_data AS (
            SELECT symbol, mstar_id
            FROM security_master
            WHERE symbol = ANY(%s)
              AND security_type IN ('OEF', 'CEF', 'ETF', 'MMF')
              AND as_of_date <= %s
            ORDER BY as_of_date DESC
            LIMIT 1
        ),
        latest_as_of_date AS (
            SELECT mstar_id, MAX(as_of_date) as as_of_date
            FROM funds_total_return_index
            WHERE mstar_id IN (SELECT mstar_id FROM security_master_data)
              AND as_of_date <= %s
            GROUP BY mstar_id
        )
        SELECT sm.symbol, f.date, f.value
        FROM funds_total_return_index f
        JOIN latest_as_of_date lad ON f.mstar_id = lad.mstar_id AND f.as_of_date = lad.as_of_date
        JOIN security_master_data sm ON f.mstar_id = sm.mstar_id
        WHERE f.date <= %s
        ORDER BY sm.symbol, f.date
        """
        
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(fund_query, (fund_symbols, as_of_date, as_of_date, as_of_date))
                fund_results = cur.fetchall()
        
        #TODO - This is not efficient. Need to improve performance.
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
        df = df.loc[df['date'].isin(trading_days)].set_index('date').sort_index()
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

def calculate_symbol_weights(holdings: List[Dict]) -> Tuple[Dict[str, Dict[str, float]], float]:
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
    
    return weights, total_value

def calculate_portfolio_return(weights: Dict[str, Dict[str, float]]) -> float:
    return sum(data['weight'] * data['return'] for data in weights.values())

def calculate_covariances(return_data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    if not return_data:
        return pd.DataFrame()  # Return an empty DataFrame if there's no data

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

def portfolio_statistics(account_id: str, as_of_date: str, return_portfolio_detail: bool = False, data: Dict = None) -> Dict[str, Union[float, Dict]]:
    if account_id == '':
        if not data or not isinstance(data, dict):
            raise ValueError("When account_id is empty, data must be provided as a non-empty dictionary.")
        
        required_keys = ['account_balance', 'holdings', 'weights', 'return_data']
        if not all(key in data for key in required_keys):
            raise ValueError(f"Data dictionary must contain all of these keys: {required_keys}")
        
        account_balance = data['account_balance']
        holdings = data['holdings']
        weights = data['weights']
        return_data = data['return_data']
        
        # Convert nested dictionary return_data back to DataFrame
        return_data = {symbol: pd.DataFrame.from_dict(data, orient='index') for symbol, data in return_data.items()}
    else:
        # Fetch all account data in one query
        holdings = fetch_account_data(account_id, as_of_date)

        # Calculate symbol weights and get individual returns and volatilities
        weights, account_balance = calculate_symbol_weights(holdings)

        # Fetch total return indices and process return data
        symbol_types = {holding['symbol']: holding['security_type'] for holding in holdings}
        total_return_indices = fetch_total_return_indices([h['symbol'] for h in holdings], symbol_types, as_of_date)
        return_data = process_return_data(total_return_indices)

    # Calculate portfolio return
    portfolio_return = calculate_portfolio_return(weights)

    # Calculate covariances
    covariance_matrix = calculate_covariances(return_data)

    # Calculate portfolio volatility
    if not covariance_matrix.empty:
        portfolio_volatility = calculate_portfolio_volatility(weights, covariance_matrix)
    else:
        portfolio_volatility = None  # or some default value

    # Extract individual security annualized returns and volatilities
    individual_stats = {
        holding['symbol']: {
            'annualized_return': holding['annualized_return'],
            'annualized_volatility': holding['annualized_volatility']
        }
        for holding in holdings
    }

    result = {
        'return': portfolio_return,
        'volatility': portfolio_volatility,
        'weights': {symbol: data['weight'] for symbol, data in weights.items()},
        'individual_stats': individual_stats,
        'account_balance': account_balance,
        'holdings': holdings
    }

    if return_portfolio_detail:
        # Convert DataFrame back to nested dictionary for JSON serialization
        result['return_data'] = {symbol: df.to_dict(orient='index') for symbol, df in return_data.items()}

    return result

def optimized_portfolio_stats(account_id: str, as_of_date: str) -> Dict[str, Union[float, Dict]]:
    # Fetch portfolio statistics with return data
    stats = portfolio_statistics(account_id, as_of_date, return_portfolio_detail=True)
    
    return_data = stats['return_data']
    weights = stats['weights']
    account_balance = stats['account_balance']
    original_holdings = stats['holdings']
    
    # Check if '$$$$' is not in weights
    if '$$$$' not in weights:
        # Fetch cash returns using VUSXX
        cash_return_indices = fetch_total_return_indices(['VUSXX'], {'VUSXX': 'MMF'}, as_of_date)
        cash_return_data = process_return_data(cash_return_indices)
        
        # Add '$$$$' to weights with zero weight
        weights['$$$$'] = 0
        
        # Add cash return data to return_data
        return_data['$$$$'] = cash_return_data['VUSXX'].to_dict(orient='index')
    
    # Convert date keys to strings in return_data
    for symbol, data in return_data.items():
        return_data[symbol] = {
            date.strftime('%Y-%m-%d') if isinstance(date, datetime.date) else str(date): values
            for date, values in data.items()
        }

    # Find common date range
    common_dates = list(set.intersection(*[set(data.keys()) for data in return_data.values()]))
    common_dates.sort()

    # Prepare symbols list ensuring '$$$$' is last
    symbols = list(weights.keys())
    symbols.remove('$$$$')
    symbols.append('$$$$')
    
    # Prepare R matrix
    R = []
    for symbol in symbols:
        R.append([return_data[symbol][date]['return'] for date in common_dates])
    R = np.array(R).T
    
    # Prepare k array
    k = np.array([weights[symbol] for symbol in symbols])
    
    alpha = 0.1  # Default regularization parameter
    cash_index = len(symbols) - 1  # Cash is always the last index
    
    optimized_portfolios = {}
    
    for cash_target in range(5, 80, 5):
        l = cash_target / 100  # Convert percentage to decimal
        c = 0.01 * l  # 1% tolerance
        
        optimizer = PortfolioOptimizer(R, k, alpha, l, c, cash_index)
        optimal_weights, optimization_result = optimizer.optimize()
        
        # Map optimal weights back to tickers
        optimal_portfolio = {
            symbol: {
                'weight': weight,
                'return': stats['individual_stats'][symbol]['annualized_return'] if symbol in stats['individual_stats'] else 0,
                'volatility': stats['individual_stats'][symbol]['annualized_volatility'] if symbol in stats['individual_stats'] else 0
            }
            for symbol, weight in zip(symbols, optimal_weights)
        }
        
        # Prepare holdings for the optimized portfolio
        optimized_holdings = []
        for symbol, data in optimal_portfolio.items():
            original_holding = next((h for h in original_holdings if h['symbol'] == symbol), None)
            if original_holding:
                quantity = data['weight'] * account_balance / original_holding['price']
                optimized_holdings.append({
                    'symbol': symbol,
                    'quantity': quantity,
                    'adjusted_cost_basis': original_holding['adjusted_cost_basis'],
                    'as_of_date': original_holding['as_of_date'],
                    'security_type': original_holding['security_type'],
                    'mstar_id': original_holding['mstar_id'],
                    'latest_market_price': original_holding['price'],
                    'annualized_return': data['return'],
                    'annualized_volatility': data['volatility']
                })
        
        portfolio_stats = portfolio_statistics(
            account_id='',
            as_of_date=as_of_date,
            return_portfolio_detail=False,
            data={
                'account_balance': account_balance,
                'holdings': optimized_holdings,
                'weights': optimal_portfolio,
                'return_data': return_data
            }
        )
        
        # Simulate balance paths for the optimized portfolio
        simulation_result = simulate_balance_paths(
            starting_balance=account_balance,
            annual_mean=portfolio_stats['return'],
            annual_std_dev=portfolio_stats['volatility'],
            start_date=as_of_date
        )

        optimized_portfolios[cash_target] = {
            'optimal_weights': {symbol: data['weight'] for symbol, data in optimal_portfolio.items()},
            'optimal_shares': {h['symbol']: h['quantity'] for h in optimized_holdings},
            'optimization_status': optimization_result.success,
            'optimization_message': optimization_result.message,
            'objective_function_value': optimization_result.fun,
            'account_balance': account_balance,
            'portfolio_return': portfolio_stats['return'],
            'portfolio_volatility': portfolio_stats['volatility'],
            'holdings': optimized_holdings,
            # Add simulation results
            'percentile_95_balance_path': simulation_result['percentile_95_balance_path'].to_dict(),
            'percentile_5_balance_path': simulation_result['percentile_5_balance_path'].to_dict(),
            'prob_95_percentile': simulation_result['prob_95_percentile'],
            'prob_5_percentile': simulation_result['prob_5_percentile'],
            'final_95_percentile_balance': simulation_result['final_95_percentile_balance'],
            'final_5_percentile_balance': simulation_result['final_5_percentile_balance'],
            'dates': simulation_result['dates']
        }
    
    # Remove return_data from the original stats
    if 'return_data' in stats:
        del stats['return_data']

    return {
        'original_stats': stats,
        'optimized_portfolios': optimized_portfolios
    }

# Example usage:
optimized_stats = optimized_portfolio_stats('7551d266-f561-4e9f-9fc1-a1faa31af499', '2024-09-07')

# Write optimized_stats to a JSON file in the @data/temp_debug folder
output_folder = '/Users/gokul/Dropbox/Mac/Documents/Personal/Prosper/prosper_backend/data/temp_debug'
os.makedirs(output_folder, exist_ok=True)
output_file = os.path.join(output_folder, 'optimized_stats.json')

with open(output_file, 'w') as f:
    json.dump(optimized_stats, f, indent=2, default=str)

print(f"Optimized stats written to {output_file}")