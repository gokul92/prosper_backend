from typing import List, Dict, Tuple, Union, Any
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
from contextlib import contextmanager
from src.domain_logic.optimization import TaxOptimizer
import logging

# Set up logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

load_dotenv()

# Database connection parameters
db_host = os.getenv('DB_HOST')
db_user = os.getenv('DB_USERNAME')
db_pass = os.getenv('DB_PASSWORD')
db_name = os.getenv('DB_NAME')
db_port = os.getenv('DB_PORT')

@contextmanager
def get_connection():
    conn = psycopg.connect(f'host={db_host} port={db_port} dbname={db_name} user={db_user} password={db_pass}',
                           row_factory=dict_row)
    try:
        yield conn
    finally:
        conn.close()

def fetch_account_data(account_id: str, as_of_date: str) -> List[Dict]:
    query = """
    WITH latest_holdings AS (
        SELECT account_id, MAX(as_of_date) as max_date
        FROM account_holdings
        WHERE account_id = %s AND as_of_date <= %s
        GROUP BY account_id
    )
    SELECT ah.symbol, ah.quantity, ah.adjusted_cost_basis, ah.as_of_date,
           ah.purchase_date, ah.capital_gains_type,
           sm.security_type, sm.mstar_id,
           CASE
               WHEN sm.security_type = 'Equity' THEN eph.close_price
               WHEN sm.security_type IN ('OEF', 'CEF', 'ETF', 'MMF') THEN fmp.day_end_market_price
               WHEN sm.security_type = 'Cash' THEN 1.0  -- Cash price is $1
               ELSE NULL
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
    ) sm ON sm.symbol = ah.symbol
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
    ) hrs ON sm.symbol = hrs.symbol
    WHERE ah.account_id = %s
    """
    
    with get_connection() as conn:
        with conn.cursor() as cur:
            params = (
                account_id, as_of_date,
                as_of_date,  # For security_master lateral join
                as_of_date, as_of_date,  # For equities_price_history lateral join
                as_of_date, as_of_date,  # For funds_market_price_and_capitalization lateral join
                as_of_date,  # For historical_return_statistics lateral join
                account_id
            )
            cur.execute(query, params)
            holdings = cur.fetchall()
    
    return holdings

def fetch_total_return_indices(symbols: List[str], symbol_types: Dict[str, str], as_of_date: str) -> Dict[str, pd.DataFrame]:
    equity_symbols = [symbol for symbol, type in symbol_types.items() if type == 'Equity']
    fund_symbols = [symbol for symbol, type in symbol_types.items() if type in ["OEF", "CEF", "ETF", "MMF", "Cash"]] # includes cash 
    
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
        placeholders = ','.join(['%s'] * len(fund_symbols))
        
        # Query 1: Get security_master data
        security_master_query = f"""
        SELECT symbol, mstar_id
        FROM security_master
        WHERE symbol IN ({placeholders})
          AND security_type IN ('OEF', 'CEF', 'ETF', 'MMF', 'Cash')
          AND as_of_date <= %s
        ORDER BY symbol, as_of_date DESC
        """
        
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(security_master_query, fund_symbols + [as_of_date])
                security_master_results = cur.fetchall()
        
        if not security_master_results:
            logger.error(f"No security master data found for symbols: {fund_symbols}")
            return {}
        
        # Query 2: Get latest as_of_date for each mstar_id
        mstar_ids = [r['mstar_id'] for r in security_master_results]
        mstar_placeholders = ','.join(['%s'] * len(mstar_ids))
        latest_as_of_date_query = f"""
        SELECT mstar_id, MAX(as_of_date) as latest_as_of_date
        FROM funds_total_return_index
        WHERE mstar_id IN ({mstar_placeholders})
          AND as_of_date <= %s
        GROUP BY mstar_id
        """
        
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(latest_as_of_date_query, mstar_ids + [as_of_date])
                latest_as_of_date_results = cur.fetchall()
        
        if not latest_as_of_date_results:
            logger.error(f"No latest as_of_date found for mstar_ids: {mstar_ids}")
            return {}
        
        # Query 3: Get the actual fund data
        fund_query = f"""
        SELECT sm.symbol, f.date, f.value
        FROM funds_total_return_index f
        JOIN (
            {security_master_query}
        ) sm ON f.mstar_id = sm.mstar_id
        JOIN (
            {latest_as_of_date_query}
        ) lad ON f.mstar_id = lad.mstar_id AND f.as_of_date = lad.latest_as_of_date
        WHERE f.date <= %s
        ORDER BY sm.symbol, f.date
        """

        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(fund_query, fund_symbols + [as_of_date] + mstar_ids + [as_of_date] + [as_of_date])
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
        # Debugging statements
        print(f"Processing symbol: {symbol}")
        print(f"Columns before reset_index: {df.columns.tolist()}")
        print(f"Index name before reset_index: {df.index.name}")

        # Reset index to make the index a column
        df = df.reset_index()

        # Identify the index column name
        index_col = df.columns[0]  # The index column becomes the first column after reset_index()
        
        # Rename the index column to 'date' if it's not already 'date'
        if index_col != 'date':
            df.rename(columns={index_col: 'date'}, inplace=True)

        # Verify that 'date' is now a column
        if 'date' not in df.columns:
            raise KeyError("The DataFrame does not contain a 'date' column after resetting index.")
        
        print(f"Columns after renaming: {df.columns.tolist()}")

        # Now proceed with processing
        df = df.sort_values(by='date', ascending=True)
        df['return'] = df['value'].pct_change()
        df = df.dropna(subset=['return'])
        
        # Remove leading rows with zero return values
        if not df['return'].ne(0).any():
            # All return values are zero, skip this symbol
            print(f"All return values are zero for {symbol}, skipping.")
            continue
        else:
            first_non_zero_index = df['return'].ne(0).idxmax()
            df = df.loc[first_non_zero_index:]
        
        # Set 'date' as the index
        df.set_index('date', inplace=True)
        
        return_data[symbol] = df
    
    return return_data

def calculate_symbol_weights(holdings: List[Dict]) -> Tuple[Dict[str, Dict[str, Union[float, None]]], float]:
    total_value = sum(holding['quantity'] * holding['price'] for holding in holdings)

    weights = {}
    for holding in holdings:
        symbol = holding['symbol']
        weight = (holding['quantity'] * holding['price']) / total_value if total_value > 0 else 0
        weight_data = {
            'weight': weight
        }

        # Include 'return' if available
        weight_data['return'] = holding.get('annualized_return')

        # Include 'volatility' if available
        weight_data['volatility'] = holding.get('annualized_volatility')

        weights[symbol] = weight_data

    return weights, total_value

def calculate_portfolio_return(weights: Dict[str, Dict[str, Union[float, None]]]) -> float:
    return sum(data['weight'] * data['return'] for data in weights.values() if data['return'] is not None)

def calculate_covariances(return_data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    if return_data:
        # Ensure all indices are of datetime type
        for symbol, data in return_data.items():
            if not np.issubdtype(data.index.dtype, np.datetime64):
                data.index = pd.to_datetime(data.index)
                return_data[symbol] = data

        # Compute the intersection of dates
        common_dates = list(set.intersection(*[set(data.index) for data in return_data.values()]))
        common_dates.sort()  # Ensure dates are in order
    else:
        common_dates = []  # or handle the empty case as appropriate for your use case
    
    # Filter returns to only include common dates
    filtered_returns = {}
    for symbol, data in return_data.items():
        # Ensure data.index is datetime and aligned with common_dates
        data = data.loc[data.index.isin(common_dates)]
        filtered_returns[symbol] = data['return']
    
    returns = pd.DataFrame(filtered_returns, index=common_dates)
    returns.sort_index(inplace=True)
    
    # Calculate the covariance matrix
    cov_matrix = returns.cov()
    
    # Annualize the covariance matrix (assuming daily returns)
    trading_days_per_year = 252  # Adjust this if using a different frequency
    cov_matrix_annual = cov_matrix * trading_days_per_year
    
    return cov_matrix_annual

def calculate_portfolio_volatility(weights: Dict[str, Dict[str, Union[float, None]]], covariance_matrix: pd.DataFrame) -> float:
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

def aggregate_holdings(holdings: List[Dict]) -> List[Dict]:
    aggregated = {}
    for holding in holdings:
        symbol = holding['symbol']
        if symbol not in aggregated:
            aggregated[symbol] = holding.copy()
            aggregated[symbol]['total_cost'] = holding['quantity'] * holding['adjusted_cost_basis']
        else:
            aggregated[symbol]['quantity'] += holding['quantity']
            aggregated[symbol]['total_cost'] += holding['quantity'] * holding['adjusted_cost_basis']
    
    for symbol, data in aggregated.items():
        data['adjusted_cost_basis'] = data['total_cost'] / data['quantity']
        del data['total_cost']
    
    return list(aggregated.values())

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
        return_data_dict = data['return_data']
        
        # Aggregate holdings if not already aggregated
        if any(h1['symbol'] == h2['symbol'] for h1 in holdings for h2 in holdings if h1 != h2):
            holdings = aggregate_holdings(holdings)
        
        # Correctly reconstruct return_data DataFrames and ensure 'return' column is present
        return_data = {}
        for symbol, data_dict in return_data_dict.items():            
            # Check if data_dict is already a DataFrame
            if isinstance(data_dict, pd.DataFrame):
                df = data_dict.copy()
            else:
                df = pd.DataFrame.from_dict(data_dict, orient='index')

            df.index = pd.to_datetime(df.index)  # Convert index to datetime
            df.sort_index(inplace=True)  # Ensure data is sorted by date
            df.reset_index(inplace=True)  # Reset index to get 'date' as a column
            df.rename(columns={'index': 'date'}, inplace=True)  # Rename 'index' column to 'date'

            # Verify if 'return' column is present
            if 'return' not in df.columns:
                # Recalculate 'return' column
                df = df.sort_values(by='date', ascending=True)
                df['return'] = df['value'].pct_change()
                df.dropna(subset=['return'], inplace=True)
                df.reset_index(drop=True, inplace=True)

            return_data[symbol] = df.set_index('date')  # Set 'date' as index for further calculations
    else:
        # Fetch all account data in one query
        raw_holdings = fetch_account_data(account_id, as_of_date)
        
        # Aggregate holdings
        holdings = aggregate_holdings(raw_holdings)

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
        'holdings': holdings,
        'return_data': return_data  # Keep DataFrames here
    }

    if return_portfolio_detail:
        # Serialize 'return_data' separately
        result['return_data_serialized'] = {symbol: df.to_dict(orient='index') for symbol, df in return_data.items()}

    return result

def optimized_portfolio_stats(account_id: str, as_of_date: str) -> Dict[str, Union[float, Dict]]:
    # Fetch portfolio statistics with return data (DataFrames)
    stats = portfolio_statistics(account_id, as_of_date, return_portfolio_detail=True)
    
    return_data = stats['return_data']  # This is now a dict of DataFrames
    weights = stats['weights']
    account_balance = stats['account_balance']
    original_holdings = stats['holdings']
    
    # Check if '$$$$' is not in weights
    if '$$$$' not in weights:

        # Fetch cash returns using $$$$
        cash_return_indices = fetch_total_return_indices(['$$$$'], {'$$$$': 'Cash'}, as_of_date)

        cash_return_data = process_return_data(cash_return_indices)
        
        # Add '$$$$' to weights with zero weight
        weights['$$$$'] = 0
        
        # Add cash return data to return_data
        return_data['$$$$'] = cash_return_data['$$$$']
    
    # Find common date range before converting return_data to dict
    common_dates = list(set.intersection(*[set(data.index) for data in return_data.values()]))
    common_dates.sort()

    # Prepare symbols list ensuring '$$$$' is last
    symbols = list(weights.keys())
    symbols.remove('$$$$')
    symbols.append('$$$$')
    
    # Prepare R matrix
    R = []
    for symbol in symbols:
        try:
            # Access 'return' column using .loc with the date index
            R.append(return_data[symbol].loc[common_dates, 'return'].values)
        except KeyError as e:
            logger.error(f"KeyError when preparing R matrix for symbol {symbol}: {str(e)}")
            logger.error(f"Return data for {symbol}: {return_data.get(symbol, 'Not found')}")
            raise
    R = np.array(R).T
    
    # Prepare k array
    k = np.array([weights[symbol] for symbol in symbols])
    
    alpha = 0.1  # Default regularization parameter
    cash_index = len(symbols) - 1  # Cash is always the last index
    
    optimized_portfolios = {}

    # Simulate balance paths for the original portfolio
    original_simulation_result = simulate_balance_paths(
        starting_balance=account_balance,
        annual_mean=stats['return'],
        annual_std_dev=stats['volatility'],
        start_date=as_of_date
    )
    
    # Add simulation results to original_stats
    stats.update({
        'percentile_95_balance_path': original_simulation_result['percentile_95_balance_path'].to_dict(),
        'percentile_5_balance_path': original_simulation_result['percentile_5_balance_path'].to_dict(),
        'prob_95_percentile': original_simulation_result['prob_95_percentile'],
        'prob_5_percentile': original_simulation_result['prob_5_percentile'],
        'final_95_percentile_balance': original_simulation_result['final_95_percentile_balance'],
        'final_5_percentile_balance': original_simulation_result['final_5_percentile_balance'],
        'dates': original_simulation_result['dates'],
        'prob_greater_than_starting': original_simulation_result['prob_greater_than_starting'],
        'prob_between_starting_and_95': original_simulation_result['prob_between_starting_and_95'],
        'prob_between_5_and_starting': original_simulation_result['prob_between_5_and_starting'],
        'percentile_95_balance_1y': original_simulation_result['percentile_95_balance_1y'],
        'percentile_5_balance_1y': original_simulation_result['percentile_5_balance_1y'],
        'expected_return_path': original_simulation_result['expected_return_path'].to_dict(),
        'prob_between_starting_and_expected': original_simulation_result['prob_between_starting_and_expected'],
        'final_expected_return_balance': original_simulation_result['final_expected_return_balance']
    })
    
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
            'percentile_95_balance_path': simulation_result['percentile_95_balance_path'].to_dict(),
            'percentile_5_balance_path': simulation_result['percentile_5_balance_path'].to_dict(),
            'prob_95_percentile': simulation_result['prob_95_percentile'],
            'prob_5_percentile': simulation_result['prob_5_percentile'],
            'final_95_percentile_balance': simulation_result['final_95_percentile_balance'],
            'final_5_percentile_balance': simulation_result['final_5_percentile_balance'],
            'dates': simulation_result['dates'],
            'prob_greater_than_starting': simulation_result['prob_greater_than_starting'],
            'prob_between_starting_and_95': simulation_result['prob_between_starting_and_95'],
            'prob_between_5_and_starting': simulation_result['prob_between_5_and_starting'],
            'percentile_95_balance_1y': simulation_result['percentile_95_balance_1y'],
            'percentile_5_balance_1y': simulation_result['percentile_5_balance_1y'],
            'expected_return_path': simulation_result['expected_return_path'].to_dict(),
            'prob_between_starting_and_expected': simulation_result['prob_between_starting_and_expected'],
            'final_expected_return_balance': simulation_result['final_expected_return_balance']
        }
    
    # After all DataFrame operations, convert date keys to strings in return_data
    for symbol, df in return_data.items():
        return_data[symbol] = df.to_dict(orient='index')
        # Convert date keys to strings
        return_data[symbol] = {
            date.strftime('%Y-%m-%d') if isinstance(date, datetime.date) else str(date): values
            for date, values in return_data[symbol].items()
        }
    
    if 'return_data' in stats:
        del stats['return_data']

    return {
        'original_stats': stats,
        'optimized_portfolios': optimized_portfolios
    }

def tax_optimized_rebalance(
    optimized_portfolio: Dict[str, Any],
    original_portfolio: Dict[str, Any],
    short_term_tax_rate: float,
    long_term_tax_rate: float
) -> Dict[str, Any]:
    trades = {}
    cash_delta = 0.0

    # Helper function to extract symbol from tax lot ID
    def extract_symbol(tax_lot_id: str) -> str:
        # Assuming tax_lot_id format is 'SYMBOL_DATE_COSTBASIS_OTHERINFO'
        return tax_lot_id.split('_')[0]

    # Group original portfolio tax lots by symbol
    original_portfolio_by_symbol = {}
    for tax_lot_id, data in original_portfolio.items():
        symbol = extract_symbol(tax_lot_id)
        if symbol not in original_portfolio_by_symbol:
            original_portfolio_by_symbol[symbol] = {
                'tax_lots': {},
                'total_shares': 0.0,
                'weight': 0.0,
                'current_price': data['current_price'],
                'security_type': data['security_type'],
                'current_price_as_of_date': data['current_price_as_of_date'],
                'as_of_date': data['as_of_date']
            }
        original_portfolio_by_symbol[symbol]['tax_lots'][tax_lot_id] = data
        shares_in_lot = sum(data.get('shares', []))
        original_portfolio_by_symbol[symbol]['total_shares'] += shares_in_lot
        original_portfolio_by_symbol[symbol]['weight'] += data.get('weight', 0.0)

    # Group optimized portfolio data by symbol
    optimized_portfolio_by_symbol = {}
    for symbol, data in optimized_portfolio.items():
        optimized_portfolio_by_symbol[symbol] = {
            'total_shares': data.get('shares', 0.0),
            'weight': data.get('weight', 0.0),
            'current_price': data.get('current_price'),
            'security_type': data.get('security_type'),
            'current_price_as_of_date': data.get('current_price_as_of_date'),
            'as_of_date': data.get('as_of_date')
        }

    # Process each symbol to calculate trades and cash delta
    all_symbols = set(original_portfolio_by_symbol.keys()).union(optimized_portfolio_by_symbol.keys())

    # Dictionary to keep track of optimized shares for each symbol
    optimized_shares = {}

    for symbol in all_symbols:
        original_data = original_portfolio_by_symbol.get(symbol, {
            'tax_lots': {},
            'total_shares': 0.0,
            'weight': 0.0,
            'current_price': None,
            'security_type': None,
            'current_price_as_of_date': None,
            'as_of_date': None
        })
        optimized_data = optimized_portfolio_by_symbol.get(symbol, {
            'total_shares': 0.0,
            'weight': 0.0,
            'current_price': None,
            'security_type': None,
            'current_price_as_of_date': None,
            'as_of_date': None
        })

        # Use data from either portfolio to fill in missing information
        current_price = optimized_data['current_price'] or original_data['current_price']
        security_type = optimized_data['security_type'] or original_data['security_type']
        current_price_as_of_date = optimized_data['current_price_as_of_date'] or original_data['current_price_as_of_date']
        as_of_date_str = optimized_data['as_of_date'] or original_data['as_of_date']
        as_of_date = datetime.datetime.strptime(as_of_date_str, '%Y-%m-%d').date() if as_of_date_str else datetime.date.today()

        weight_in_original = original_data['weight']
        weight_in_optimized = optimized_data['weight']
        shares_in_original = original_data['total_shares']
        shares_in_optimized = optimized_data['total_shares']

        delta_shares = shares_in_optimized - shares_in_original
        cash_delta -= delta_shares * current_price  # Update cash position

        optimized_shares[symbol] = shares_in_optimized  # Keep track of optimized shares

        tax_lots = {}
        total_taxes = 0.0  # To accumulate total taxes for this symbol

        if delta_shares < 0:  # Selling shares
            total_shares_to_sell = abs(delta_shares)
            # Prepare data for TaxOptimizer
            s = current_price
            q_m = []
            c = []
            r = []
            tax_lot_ids = []
            lot_shares_list = []
            purchase_dates = []  # Added for purchase_date
            capital_gains_types = []  # Added for capital_gains_type

            # Iterate over each tax lot
            for tax_lot_id, lot_data in original_data['tax_lots'].items():
                shares_list = lot_data.get('shares', [])
                cost_basis_list = lot_data.get('adjusted_cost_basis', [])
                purchase_dates_list = lot_data.get('purchase_date', [])
                capital_gains_types_list = lot_data.get('capital_gains_type', [])

                # Ensure lists are of same length
                num_entries = len(shares_list)
                cost_basis_list = cost_basis_list[:num_entries]
                purchase_dates_list = purchase_dates_list[:num_entries]
                capital_gains_types_list = capital_gains_types_list[:num_entries]

                for i in range(num_entries):
                    shares = shares_list[i]
                    cost_basis = cost_basis_list[i]
                    purchase_date = purchase_dates_list[i] if i < len(purchase_dates_list) else None
                    capital_gains_type = capital_gains_types_list[i] if i < len(capital_gains_types_list) else None

                    # Determine the tax rate r
                    if purchase_date:
                        if isinstance(purchase_date, str):
                            try:
                                purchase_date_dt = datetime.datetime.strptime(purchase_date, '%Y-%m-%d').date()
                            except ValueError:
                                raise ValueError(f"Invalid purchase_date format for tax lot {tax_lot_id}: {purchase_date}")
                        else:
                            purchase_date_dt = purchase_date
                        days_held = (as_of_date - purchase_date_dt).days
                        if days_held > 365:
                            tax_rate = long_term_tax_rate
                        else:
                            tax_rate = short_term_tax_rate
                    elif capital_gains_type:
                        if capital_gains_type == 'lt':
                            tax_rate = long_term_tax_rate
                        elif capital_gains_type == 'st':
                            tax_rate = short_term_tax_rate
                        else:
                            raise ValueError(f"Invalid capital_gains_type '{capital_gains_type}' for tax lot {tax_lot_id}")
                    else:
                        raise ValueError(f"Missing capital_gains_type and purchase_date for tax lot {tax_lot_id}")

                    q_m.append(shares)
                    c.append(cost_basis)
                    r.append(tax_rate)
                    tax_lot_ids.append(tax_lot_id)
                    lot_shares_list.append(shares)
                    purchase_dates.append(purchase_date)
                    capital_gains_types.append(capital_gains_type)

            q = sum(q_m)
            delta = total_shares_to_sell

            # Ensure all arrays are numpy arrays
            q_m = np.array(q_m)
            c = np.array(c)
            r = np.array(r)

            # Initialize TaxOptimizer
            tax_optimizer = TaxOptimizer(delta=delta, s=s, c=c, r=r, q=q, q_m=q_m)
            optimal_fractions, optimization_result = tax_optimizer.optimize()

            if not optimization_result.success:
                raise ValueError(f"Tax optimization failed for symbol {symbol}: {optimization_result.message}")

            # Compute shares to sell from each lot
            shares_to_sell_per_lot = optimal_fractions * q_m
            # Compute taxes incurred from each lot
            taxes_per_lot = shares_to_sell_per_lot * (s - c) * r
            total_taxes = np.sum(taxes_per_lot)

            # Build tax_lot entries
            for i, (tax_lot_id, shares_to_sell, lot_shares, cost_basis, tax_incurred, purchase_date, capital_gains_type) in enumerate(zip(tax_lot_ids, shares_to_sell_per_lot, lot_shares_list, c, taxes_per_lot, purchase_dates, capital_gains_types)):
                if shares_to_sell > 0:
                    gain_loss = (s - cost_basis) * shares_to_sell  # Added gain/loss calculation
                    tax_lots[tax_lot_id] = {
                        "adjusted_cost_basis": cost_basis,
                        "number_of_shares_in_original_portfolio": lot_shares,
                        "number_of_shares_in_optimized_portfolio": lot_shares - shares_to_sell,
                        "number_of_shares_traded": -shares_to_sell,
                        "taxes_incurred": tax_incurred,
                        "purchase_date": purchase_date,  # Added purchase_date
                        "capital_gains_type": capital_gains_type,  # Added capital_gains_type
                        "gain_loss": gain_loss  # Added gain/loss
                    }

        elif delta_shares > 0:  # Buying shares
            # Create a new tax lot
            tax_lot_id = f"{symbol}_{as_of_date_str}_{current_price}_new"
            tax_lots[tax_lot_id] = {
                "adjusted_cost_basis": current_price,
                "number_of_shares_in_original_portfolio": 0.0,
                "number_of_shares_in_optimized_portfolio": delta_shares,
                "number_of_shares_traded": delta_shares,
                "taxes_incurred": 0.0,  # No taxes incurred when buying
                "purchase_date": as_of_date_str,  # Added purchase_date
                "capital_gains_type": None,  # No capital gains type for new purchases
                "gain_loss": 0.0  # No gain/loss on purchase
            }

        else:
            continue  # No trades required

        # Build the trades entry for the symbol
        trades[symbol] = {
            "symbol": symbol,
            "current_price": current_price,
            "security_type": security_type,
            "current_price_as_of_date": current_price_as_of_date,
            "as_of_date": as_of_date_str,
            "weight_in_original_portfolio": weight_in_original,
            "weight_in_optimized_portfolio": weight_in_optimized,  # Will update later
            "number_of_shares_traded": delta_shares,
            "tax_lots": tax_lots
        }
        if delta_shares < 0:
            trades[symbol]["taxes_incurred"] = total_taxes

    # Handle cash
    cash_symbol = '$$$$'
    cash_original = original_portfolio.get(cash_symbol, {})
    cash_shares_original = cash_original.get('total_shares', 0.0)
    cash_shares_optimized = cash_shares_original + cash_delta  # Updated cash position

    optimized_shares[cash_symbol] = cash_shares_optimized  # Keep track of optimized cash shares

    # Calculate total optimized portfolio value
    total_optimized_value = 0.0
    for symbol, shares in optimized_shares.items():
        price = optimized_portfolio_by_symbol.get(symbol, {}).get('current_price', 1.0)  # Default price for cash is 1.0
        total_optimized_value += shares * price

    # Update 'weight_in_optimized_portfolio' for each symbol
    for symbol, trade_data in trades.items():
        shares = optimized_shares.get(symbol, 0.0)
        price = trade_data['current_price']
        position_value = shares * price
        weight_in_optimized = position_value / total_optimized_value if total_optimized_value != 0 else 0.0
        trade_data['weight_in_optimized_portfolio'] = weight_in_optimized

    # Update cash position in trades with recalculated weight
    cash_position_value = optimized_shares[cash_symbol]  # cash_shares_optimized
    weight_in_original_cash = cash_original.get('weight', 0.0)
    weight_in_optimized_cash = cash_position_value / total_optimized_value if total_optimized_value != 0 else 0.0
    trades[cash_symbol] = {
        "symbol": cash_symbol,
        "current_price": 1.0,
        "security_type": "MMF",
        "current_price_as_of_date": cash_original.get('current_price_as_of_date', ''),
        "as_of_date": cash_original.get('as_of_date', ''),
        "weight_in_original_portfolio": weight_in_original_cash,
        "weight_in_optimized_portfolio": weight_in_optimized_cash,
        "number_of_shares_traded": cash_delta,
        "tax_lots": {},
        "purchase_date": None,  # No purchase date for cash
        "capital_gains_type": None,  # No capital gains type for cash
        "gain_loss": 0.0  # No gain/loss for cash
    }

    return {
        "status": "success",
        "tax_optimal_trades": trades
    }

def fetch_portfolio_data(as_of_date: str, portfolio_id: str = None, account_id: str = None) -> List[Dict]:
    """
    Fetches portfolio data based on portfolio_id and/or account_id for a given as_of_date.
    Modified to include price and other fields similar to fetch_account_data.
    
    Args:
        as_of_date (str): The date for which to fetch portfolio data in 'YYYY-MM-DD' format.
        portfolio_id (str, optional): The portfolio ID to filter the data.
        account_id (str, optional): The account ID to filter the data.
    
    Returns:
        List[Dict]: A list of portfolio holdings with additional data.
    
    Raises:
        ValueError: If neither portfolio_id nor account_id is provided.
        Exception: For any database-related errors.
    """
    if not portfolio_id and not account_id:
        raise ValueError("Either portfolio_id or account_id must be provided.")
    
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                # Construct query based on provided IDs
                if portfolio_id:
                    id_field = 'portfolio_id'
                    id_value = portfolio_id
                else:
                    id_field = 'account_id'
                    id_value = account_id

                query = f"""
                WITH latest_holdings AS (
                    SELECT {id_field}, MAX(as_of_date) as max_date
                    FROM portfolio_holdings
                    WHERE {id_field} = %s AND as_of_date <= %s
                    GROUP BY {id_field}
                )
                SELECT ph.symbol, ph.quantity, ph.as_of_date,
                       sm.security_type, sm.mstar_id,
                       CASE
                           WHEN sm.security_type = 'Equity' THEN eph.close_price
                           WHEN sm.security_type IN ('OEF', 'CEF', 'ETF', 'MMF') THEN fmp.day_end_market_price
                           WHEN sm.security_type = 'Cash' THEN 1.0  -- Cash price is $1
                           ELSE NULL
                       END as price
                FROM portfolio_holdings ph
                JOIN latest_holdings lh ON ph.{id_field} = lh.{id_field} AND ph.as_of_date = lh.max_date
                LEFT JOIN LATERAL (
                    SELECT symbol, security_type, mstar_id
                    FROM security_master
                    WHERE symbol = ph.symbol AND as_of_date <= %s
                    ORDER BY as_of_date DESC
                    LIMIT 1
                ) sm ON sm.symbol = ph.symbol
                LEFT JOIN LATERAL (
                    SELECT ticker, close_price
                    FROM equities_price_history
                    WHERE ticker = ph.symbol AND date <= %s AND as_of_date <= %s
                    ORDER BY date DESC, as_of_date DESC
                    LIMIT 1
                ) eph ON sm.security_type = 'Equity'
                LEFT JOIN LATERAL (
                    SELECT ticker, day_end_market_price
                    FROM funds_market_price_and_capitalization
                    WHERE ticker = ph.symbol AND date <= %s AND as_of_date <= %s
                    ORDER BY date DESC, as_of_date DESC
                    LIMIT 1
                ) fmp ON sm.security_type IN ('OEF', 'CEF', 'ETF', 'MMF')
                WHERE ph.{id_field} = %s
                """

                params = (
                    id_value, as_of_date,
                    as_of_date,  # For security_master lateral join
                    as_of_date, as_of_date,  # For equities_price_history lateral join
                    as_of_date, as_of_date,  # For funds_market_price_and_capitalization lateral join
                    id_value
                )

                cur.execute(query, params)
                holdings = cur.fetchall()

                if not holdings:
                    logger.warning(f"No portfolio holdings found for {id_field} '{id_value}' on or before {as_of_date}.")
                    return []

                return [dict(row) for row in holdings]

    except psycopg.Error as e:
        logger.error(f"Database error occurred: {e}")
        raise Exception("An error occurred while fetching portfolio data.") from e
    except Exception as ex:
        logger.error(f"An unexpected error occurred: {ex}")
        raise

def fetch_fund_asset_allocation(symbol: str, as_of_date: str) -> Dict[str, Any]:
    """
    Fetches the asset allocation data for a given fund symbol as of a specific date.
    
    Args:
        symbol (str): The ticker symbol of the fund.
        as_of_date (str): The date for which to fetch the data in 'YYYY-MM-DD' format.
    
    Returns:
        Dict[str, Any]: A dictionary containing the asset allocation data.
    """
    query = """
    WITH latest_data AS (
        SELECT mstar_id, MAX(as_of_date) as max_date
        FROM funds_asset_allocation
        WHERE ticker = %s AND as_of_date <= %s
        GROUP BY mstar_id
    )
    SELECT faa.*
    FROM funds_asset_allocation faa
    INNER JOIN latest_data ld ON faa.mstar_id = ld.mstar_id AND faa.as_of_date = ld.max_date
    WHERE faa.ticker = %s AND faa.as_of_date = ld.max_date
    LIMIT 1
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (symbol, as_of_date, symbol))
            result = cur.fetchone()
            if result:
                return dict(result)
            else:
                return None

# TODO: Add support for $$$$ cash treatment
def calculate_asset_allocation(
    as_of_date: str,
    argument_type: str,
    argument_id: str
) -> Dict[str, float]:
    """
    Calculates the asset allocation for a given portfolio or account holding.
    
    Args:
        as_of_date (str): The date for which to perform the calculation in 'YYYY-MM-DD' format.
        argument_type (str): 'account' or 'portfolio' indicating the type of ID provided.
        argument_id (str): The account_id or portfolio_id based on the argument_type.
    
    Returns:
        Dict[str, float]: A dictionary with asset allocation percentages for each component.
    
    Raises:
        ValueError: If invalid argument_type is provided or if required data is missing.
        Exception: For any database or data retrieval related errors.
    """
    if argument_type not in ('account', 'portfolio'):
        raise ValueError("argument_type must be either 'account' or 'portfolio'.")

    # Fetch holdings based on argument_type
    if argument_type == 'account':
        raw_holdings = fetch_account_data(account_id=argument_id, as_of_date=as_of_date)
        # Aggregate holdings for accounts
        holdings = aggregate_holdings(raw_holdings)
    elif argument_type == 'portfolio':
        holdings = fetch_portfolio_data(as_of_date=as_of_date, portfolio_id=argument_id)
    else:
        raise ValueError("Invalid argument_type provided.")

    if not holdings:
        raise ValueError(f"No holdings found for {argument_type}_id '{argument_id}' on or before {as_of_date}.")

    # Ensure holdings have the required data
    required_fields = ['symbol', 'quantity', 'price', 'security_type']
    for holding in holdings:
        for field in required_fields:
            if field not in holding or holding[field] is None:
                raise ValueError(f"Missing required field '{field}' in holdings data for symbol '{holding.get('symbol', 'unknown')}'.")

    # Calculate weights of each security
    weights, _ = calculate_symbol_weights(holdings)

    print(f"Weights within asset allocation: {weights}")

    # Initialize asset allocation components
    asset_allocation = {
        'bond': 0.0,
        'cash': 0.0,
        'convertible': 0.0,
        'other': 0.0,
        'preferred': 0.0,
        'stock': 0.0
    }

    for holding in holdings:
        symbol = holding['symbol']
        security_type = holding['security_type']
        weight = weights[symbol]['weight']

        if security_type == 'Equity':
            # Equity is classified as 100% stock
            allocation = {
                'bond': 0.0,
                'cash': 0.0,
                'convertible': 0.0,
                'other': 0.0,
                'preferred': 0.0,
                'stock': 100.0
            }
        elif security_type in ('ETF', 'MMF', 'OEF', 'CEF', 'Cash'):
            # Fetch asset allocation from funds_asset_allocation table
            fund_allocation = fetch_fund_asset_allocation(
                symbol=symbol,
                as_of_date=as_of_date
            )
            if not fund_allocation:
                raise ValueError(f"Asset allocation data not found for fund symbol '{symbol}'.")

            # Ensure all required asset allocation fields are present
            allocation_fields = ['bond_net', 'cash_net', 'convertible_net', 'other_net', 'preferred_net', 'stock_net']
            if not all(field in fund_allocation and fund_allocation[field] is not None for field in allocation_fields):
                raise ValueError(f"Asset allocation data incomplete for fund symbol '{symbol}'.")

            allocation = {
                'bond': fund_allocation['bond_net'],
                'cash': fund_allocation['cash_net'],
                'convertible': fund_allocation['convertible_net'],
                'other': fund_allocation['other_net'],
                'preferred': fund_allocation['preferred_net'],
                'stock': fund_allocation['stock_net']
            }
        else:
            raise ValueError(f"Unsupported security type '{security_type}' for symbol '{symbol}'.")

        # Weighted average calculation
        for component in asset_allocation:
            asset_allocation[component] += weight * allocation[component] / 100.0  # Convert percentage to fraction

    # Convert fractions back to percentages
    for component in asset_allocation:
        asset_allocation[component] *= 100.0

    return asset_allocation