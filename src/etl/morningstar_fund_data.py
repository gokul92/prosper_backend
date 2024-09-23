import os
import re
from typing import List, Dict, Union
from datetime import datetime
import numpy as np
import pandas as pd
from dotenv import load_dotenv
from db_utils import add_to_db, get_connection
import csv
import time
import pandas_market_calendars as mcal
import concurrent.futures
from concurrent.futures import ProcessPoolExecutor
from functools import partial
from tqdm import tqdm
from morningstar_equity_data import log_to_csv
import logging

load_dotenv()

db_host = os.getenv('DB_HOST')
db_user = os.getenv('DB_USERNAME')
db_pass = os.getenv('DB_PASSWORD')
db_name = os.getenv('DB_NAME')
db_port = os.getenv('DB_PORT')
ftp_user = os.getenv('MSTAR_FTP_USERNAME')
ftp_pass = os.getenv('MSTAR_FTP_PASSWORD')

common_cols = [ 'MStarID', 'Ticker', 'FundName', 'PortfolioDate' ]

"""
# LATER - Add function to read raw files from ftp and save in S3
# LATER - Create a droplet in digital ocean to parse the files and save in postgres tables
"""
yields_and_expenses_cols = [ 'NetExpenseRatio', 'SECYield', 'SECYieldDate',
                             'CategoryName', 'CategoryNetExpenseRatio', 'Yield1Yr', 'Yield1YrDate' ]

yields_and_expenses_numeric_cols = [ 'NetExpenseRatio', 'SECYield', 'CategoryNetExpenseRatio', 'Yield1Yr' ]

market_price_and_cap_cols = [ 'DayEndMarketPrice', 'DayEndMarketPriceDate', 'SharesOutstanding',
                              'SharesOutstandingDate', 'DayEndNAV', 'DayEndNAVDate' ]

market_price_and_cap_numeric_cols = [ 'DayEndMarketPrice', 'SharesOutstanding', 'DayEndNAV' ]

categories_cols = [ 'BroadCategoryGroup', 'BroadCategoryGroupID',
                    'CategoryName', 'BroadAssetClass', 'PrimaryIndexId', 'PrimaryIndexName' ]

bond_sector_cols = [ 'SectorAgencyMortgageBacked',
                     'SectorAssetBacked',
                     'SectorBankLoan', 'SectorCash', 'SectorCommercialMortgageBacked',
                     'SectorConvertible', 'SectorCorporate', 'SectorCovered', 'SectorFuture',
                     'SectorGovernment', 'SectorGovernmentRelated', 'SectorMunicipal',
                     'SectorMunicipalTaxAdvantaged',
                     'SectorNonAgencyResidentialMortgageBacked', 'SectorOption',
                     'SectorPreferred', 'SectorSwap' ]

super_sector_cols = [ 'SuperSectorCash',
                      'SuperSectorCorporate', 'SuperSectorDerivative',
                      'SuperSectorGovernment', 'SuperSectorMunicipal',
                      'SuperSectorSecuritized' ]

asset_allocation_cols = [ 'BondLong', 'BondNet', 'BondShort',
                          'CashLong', 'CashNet', 'CashShort', 'ConvertibleLong', 'ConvertibleNet',
                          'ConvertibleShort', 'OtherLong', 'OtherNet', 'OtherShort',
                          'PreferredLong', 'PreferredNet', 'PreferredShort',
                          'StockLong', 'StockNet', 'StockShort' ]

net_asset_allocation_cols = [ 'BondNet', 'CashNet', 'ConvertibleNet', 'OtherNet', 'PreferredNet', 'StockNet' ]

stock_sector_cols = [ 'BasicMaterials', 'CommunicationServices', 'ConsumerCyclical',
                      'ConsumerDefensive', 'Energy', 'FinancialServices', 'Healthcare',
                      'Industrials', 'RealEstate', 'Technology', 'Utilities' ]

us_non_us_breakdown_cols = [ 'NonUSBondLong', 'NonUSBondNet', 'NonUSBondShort',
                             'NonUSStockLong', 'NonUSStockNet', 'NonUSStockShort',
                             'USBondLong', 'USBondNet',
                             'USBondShort', 'USStockLong', 'USStockNet', 'USStockShort' ]

us_non_us_net_cols = [ 'NonUSBondNet', 'NonUSStockNet', 'USBondNet', 'USStockNet' ]


def fetch_similar_funds(mstar_id: str) -> List[ Dict ]:
    """
    Fetch funds similar to the mstar_id passed in.

    :param mstar_id: The ticker mstar_id of the fund
    :return: A list of dictionaries containing information about similar funds
    """
    conn = get_connection()
    cursor = conn.cursor()

    try:
        # Step 1: Retrieve the category of the mstar_id
        category_query = """
        WITH latest_category AS (
            SELECT mstar_id, ticker, MAX(as_of_date) as latest_date
            FROM funds_categories
            WHERE mstar_id = %s
            GROUP BY mstar_id
        )
        SELECT fc.category_name, fc.security_type
        FROM funds_categories fc
        JOIN latest_category lc ON fc.mstar_id = lc.mstar_id AND fc.as_of_date = lc.latest_date
        WHERE fc.mstar_id = %s
        """
        cursor.execute(category_query, (mstar_id, mstar_id))
        category_result = cursor.fetchone()

        if not category_result:
            return [ ]  # Return empty list if the mstar_id's category is not found

        category_name, security_type = category_result[ 'category_name' ], category_result[ 'security_type' ]

        # Step 2 & 3: Retrieve list of ETFs in that category excluding the mstar_id
        similar_funds_query = """
        WITH latest_category AS (
            SELECT mstar_id, ticker, MAX(as_of_date) as latest_date
            FROM funds_categories
            GROUP BY mstar_id
        ), latest_market_cap AS (
            SELECT mstar_id, ticker, MAX(as_of_date) as latest_date
            FROM funds_market_price_and_capitalization
            GROUP BY mstar_id
        )
        SELECT fc.ticker, fc.mstar_id, fc.fund_name, mc.market_capitalization
        FROM funds_categories fc
        JOIN latest_category lc ON fc.mstar_id = lc.mstar_id AND fc.as_of_date = lc.latest_date
        JOIN funds_market_price_and_capitalization mc ON fc.mstar_id = mc.mstar_id
        JOIN latest_market_cap lmc ON mc.mstar_id = lmc.mstar_id AND mc.as_of_date = lmc.latest_date
        WHERE fc.category_name = %s AND fc.security_type = 'ETF' AND fc.mstar_id != %s
        ORDER BY mc.market_capitalization DESC
        LIMIT 2
        """
        cursor.execute(similar_funds_query, (category_name, mstar_id))
        similar_funds = cursor.fetchall()

        # Step 4 & 5: Format and return the results
        result = [ ]
        for fund in similar_funds:
            result.append({
                'mstar_id': fund[ 'mstar_id' ],
                'symbol': fund[ 'ticker' ],
                'fund_name': fund[ 'fund_name' ],
                'market_cap': fund[ 'market_capitalization' ],
                'category_name': category_name
            })

        return result  # This will be an empty list if no similar funds are found

    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return [ ]
    finally:
        cursor.close()
        conn.close()

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

def fetch_fund_data(mstar_ids: List[str], table_name: str, as_of_date: Union[str, datetime], start_date: Union[str, datetime, None] = None) -> List[Dict]:
    """
    Fetch fund data for given Morningstar IDs, date range, and as_of_date.

    :param mstar_ids: List of Morningstar IDs of the funds
    :param table_name: The name of the table to fetch data from
    :param as_of_date: The as-of date for the data (equivalent to end_date)
    :param start_date: The start date of the data range. If None, fetch longest history possible.
    :return: A list of dictionaries containing the fund data
    """
    conn = get_connection()
    cursor = conn.cursor()

    try:
        # Validate table name to prevent SQL injection
        valid_tables = [
            'funds_asset_allocation', 'funds_bond_sector', 'funds_categories',
            'funds_market_price_and_capitalization', 'funds_stock_sector',
            'funds_super_sector', 'funds_us_non_us', 'funds_yields_and_expenses',
            'funds_total_return_index'
        ]
        if table_name not in valid_tables:
            return [{"error": f"Invalid table name: {table_name}"}]

        # Convert dates to datetime objects if they're strings
        if isinstance(as_of_date, str):
            as_of_date = datetime.strptime(as_of_date, '%Y-%m-%d').date()
        if isinstance(start_date, str):
            start_date = datetime.strptime(start_date, '%Y-%m-%d').date()

        if table_name == 'funds_total_return_index':
            if start_date is None:
                query = f"""
                WITH latest_as_of_date AS (
                    SELECT MAX(as_of_date) as max_date
                    FROM {table_name}
                    WHERE mstar_id = ANY(%s)
                      AND as_of_date <= %s
                )
                SELECT * FROM {table_name}
                WHERE mstar_id = ANY(%s)
                  AND date <= %s
                  AND as_of_date = (SELECT max_date FROM latest_as_of_date)
                ORDER BY mstar_id, date
                """
                logger.debug(f"Executing query: {query}")
                logger.debug(f"Query parameters: {(mstar_ids, as_of_date, mstar_ids, as_of_date)}")
                cursor.execute(query, (mstar_ids, as_of_date, mstar_ids, as_of_date))
            else:
                query = f"""
                WITH latest_as_of_date AS (
                    SELECT MAX(as_of_date) as max_date
                    FROM {table_name}
                    WHERE mstar_id = ANY(%s)
                      AND as_of_date <= %s
                )
                SELECT * FROM {table_name}
                WHERE mstar_id = ANY(%s)
                  AND date BETWEEN %s AND %s
                  AND as_of_date = (SELECT max_date FROM latest_as_of_date)
                ORDER BY mstar_id, date
                """
                logger.debug(f"Executing query: {query}")
                logger.debug(f"Query parameters: {(mstar_ids, as_of_date, mstar_ids, start_date, as_of_date)}")
                cursor.execute(query, (mstar_ids, as_of_date, mstar_ids, start_date, as_of_date))
        else:
            query = f"""
            WITH latest_as_of_date AS (
                SELECT mstar_id, MAX(as_of_date) as max_date
                FROM {table_name}
                WHERE mstar_id = ANY(%s)
                  AND as_of_date <= %s
                GROUP BY mstar_id
            )
            SELECT t.* FROM {table_name} t
            JOIN latest_as_of_date l ON t.mstar_id = l.mstar_id AND t.as_of_date = l.max_date
            WHERE t.mstar_id = ANY(%s)
            ORDER BY t.mstar_id
            """
            logger.debug(f"Executing query: {query}")
            logger.debug(f"Query parameters: {(mstar_ids, as_of_date, mstar_ids)}")
            cursor.execute(query, (mstar_ids, as_of_date, mstar_ids))

        result = cursor.fetchall()
        logger.debug(f"Query returned {len(result)} rows")

        if not result:
            return [{"error": f"No data found for Morningstar IDs {mstar_ids} in table {table_name}"}]

        return [dict(row) for row in result]

    except Exception as e:
        logger.exception(f"An error occurred while fetching fund data: {str(e)}")
        return [{"error": str(e)}]
    finally:
        cursor.close()
        conn.close()


def to_snake_case(string):
    # Step 1: Handle special cases like 'MStarID' and 'CUSIP'
    special_cases = {
        'MStarID': 'mstar_id',
        'SECYield': 'sec_yield',
        'SECYieldDate': 'sec_yield_date',
        'PortfolioDate': 'date',
        'Yield1Yr': 'yield_1_yr',
        'Yield1YrDate': 'yield_1_yr_date',
        'DayEndNAV': 'day_end_market_price'
    }
    if string in special_cases:
        return special_cases[ string ]

    # Step 2: Insert underscore between adjacent lowercase and uppercase letters
    string = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', string)

    # Step 3: Insert underscore between lowercase and uppercase letters
    string = re.sub('([a-z0-9])([A-Z])', r'\1_\2', string)

    # Step 4: Convert to lowercase
    return string.lower()


def raw_df_processing(raw_df):
    df = raw_df.copy()

    # remove empty tickers
    df = df.loc[ ~df[ 'Ticker' ].isin([ '', ' ' ]) ]

    # remove tickers without a portfolio date
    df = df.loc[ ~df[ 'PortfolioDate' ].isin([ '', ' ' ]) ]

    numeric_cols = (yields_and_expenses_numeric_cols + market_price_and_cap_numeric_cols + bond_sector_cols +
                    super_sector_cols + asset_allocation_cols + stock_sector_cols + us_non_us_breakdown_cols)
    df[ numeric_cols ] = df[ numeric_cols ].apply(pd.to_numeric, errors='coerce')

    # remove tickers with all net asset allocation columns = null
    df = df.dropna(subset=net_asset_allocation_cols, how='all')
    return df

def characteristics_processing(df, as_of_date, sec_type, characteristic_type, table_name):
    if characteristic_type == 'asset_allocation':
        processed_df = df[ common_cols + asset_allocation_cols ]
        processed_df[ 'AssetAllocationSum' ] = processed_df[ net_asset_allocation_cols ].sum(
            axis=1).round(2)
        processed_df[ 'UnclassifiedNet' ] = 100.0 - processed_df[ 'AssetAllocationSum' ]
        processed_df.drop('AssetAllocationSum', axis=1, inplace=True)
        processed_df[ asset_allocation_cols ] = processed_df[ asset_allocation_cols ].fillna(0.0)
    elif characteristic_type == 'stock_sector':
        processed_df = df[ common_cols + stock_sector_cols ]
        processed_df = processed_df.dropna(subset=stock_sector_cols, how='all')
        processed_df[ 'StockSectorAllocationSum' ] = processed_df[ stock_sector_cols ].sum(axis=1).round(2)
        processed_df[ 'Unclassified' ] = 100.0 - processed_df[ 'StockSectorAllocationSum' ]
        processed_df.drop('StockSectorAllocationSum', axis=1, inplace=True)
        processed_df[ stock_sector_cols ] = processed_df[ stock_sector_cols ].fillna(0.0)
    elif characteristic_type == 'us_non_us':
        processed_df = df[ common_cols + us_non_us_breakdown_cols ]
        processed_df = processed_df.dropna(subset=us_non_us_net_cols, how='all')
        processed_df[ 'USNonUSAllocationSum' ] = processed_df[ us_non_us_net_cols ].sum(axis=1).round(2)
        processed_df[ 'Other' ] = 100.0 - processed_df[ 'USNonUSAllocationSum' ]
        processed_df.drop('USNonUSAllocationSum', axis=1, inplace=True)
        processed_df[ us_non_us_breakdown_cols ] = processed_df[ us_non_us_breakdown_cols ].fillna(0.0)
    elif characteristic_type == 'super_sector':
        processed_df = df[ common_cols + super_sector_cols ]
        processed_df = processed_df.dropna(subset=super_sector_cols, how='all')
        processed_df[ 'SuperSectorAllocationSum' ] = processed_df[ super_sector_cols ].sum(axis=1).round(2)
        processed_df[ 'OtherSuperSector' ] = 100.0 - processed_df[ 'SuperSectorAllocationSum' ]
        processed_df.drop('SuperSectorAllocationSum', axis=1, inplace=True)
        processed_df[ super_sector_cols ] = processed_df[ super_sector_cols ].fillna(0.0)
    elif characteristic_type == 'bond_sector':
        processed_df = df[ common_cols + bond_sector_cols ]
        processed_df = processed_df.dropna(subset=bond_sector_cols, how='all')
        processed_df[ 'BondSectorAllocationSum' ] = processed_df[ bond_sector_cols ].sum(axis=1).round(2)
        processed_df[ 'SectorUnclassified' ] = 100.0 - processed_df[ 'BondSectorAllocationSum' ]
        processed_df.drop('BondSectorAllocationSum', axis=1, inplace=True)
        processed_df[ bond_sector_cols ] = processed_df[ bond_sector_cols ].fillna(0.0)
    elif characteristic_type == 'categories':
        processed_df = df[ common_cols + categories_cols ]
        processed_df[ categories_cols ] = processed_df[ categories_cols ].replace(" ", "")
    elif characteristic_type == 'yields_and_expenses':
        processed_df = df[ common_cols + yields_and_expenses_cols ]
        processed_df = processed_df.dropna(subset=yields_and_expenses_cols, how='all')
        processed_df.loc[ processed_df[ 'SECYieldDate' ].isin([ '', ' ' ]), 'SECYieldDate' ] = np.nan
        processed_df.loc[ processed_df[ 'Yield1YrDate' ].isin([ '', ' ' ]), 'Yield1YrDate' ] = np.nan
        processed_df[ 'SECYieldDate' ].fillna(processed_df[ 'PortfolioDate' ], inplace=True)
        processed_df[ 'Yield1YrDate' ].fillna(processed_df[ 'PortfolioDate' ], inplace=True)
    elif characteristic_type == 'market_price_and_cap':
        processed_df = df[ common_cols + market_price_and_cap_cols ]
        for col in market_price_and_cap_numeric_cols:
            processed_df[col] = pd.to_numeric(processed_df[col], errors='coerce')
        processed_df = processed_df.dropna(subset=market_price_and_cap_cols, how='all')
        processed_df.loc[ processed_df[ 'DayEndMarketPriceDate' ].isin([ '', ' ' ]), 'DayEndMarketPriceDate' ] = np.nan
        processed_df.loc[ processed_df[ 'DayEndNAVDate' ].isin([ '', ' ' ]), 'DayEndNAVDate' ] = np.nan
        processed_df.loc[processed_df['SharesOutstandingDate'].isin([ '', ' ' ]), 'SharesOutstandingDate'] = np.nan
        processed_df[ 'DayEndMarketPriceDate' ].fillna(processed_df[ 'PortfolioDate' ], inplace=True)
        processed_df[ 'DayEndNAVDate' ].fillna(processed_df[ 'PortfolioDate' ], inplace=True)
        processed_df['SharesOutstandingDate'].fillna(processed_df['PortfolioDate'], inplace=True)
        if sec_type in ['OEF', 'MMF']:
            processed_df['MarketCapitalization'] = processed_df['DayEndNAV'] * processed_df['SharesOutstanding']
            processed_df['DayEndMarketPrice'] = processed_df['DayEndNAV']
            processed_df['DayEndMarketPriceDate'] = processed_df['DayEndNAVDate']
        elif sec_type in ['ETF', 'CEF']:
            processed_df[ 'MarketCapitalization' ] = processed_df[ 'DayEndMarketPrice' ] * processed_df[
                'SharesOutstanding' ]
        processed_df.drop(columns=['DayEndNAV', 'DayEndNAVDate'], inplace=True)
        date_cols = ['PortfolioDate', 'DayEndMarketPriceDate', 'SharesOutstandingDate']
        for col in date_cols:
            if col in processed_df.columns:
                processed_df[col] = pd.to_datetime(processed_df[col], errors='coerce')
                processed_df[col] = processed_df[col].dt.strftime('%Y-%m-%d')
        
        # Drop any remaining rows with NaT or NaN in date columns
        processed_df = processed_df.dropna(subset=[col for col in date_cols if col in processed_df.columns])

    if 'processed_df' in locals():
        renamed_col_names = {}
        for col in processed_df.columns:
            renamed_col_names[ col ] = to_snake_case(col)

        processed_df[ 'as_of_date' ] = as_of_date

        if sec_type in [ 'OEF', 'CEF', 'ETF', 'MMF' ]:
            processed_df[ 'security_type' ] = sec_type

        processed_df = processed_df.rename(columns=renamed_col_names)

        add_to_db(processed_df, table_name)


def performance_processing(filename: str, as_of_date: str, sec_type: str, table_name: str):
    """
    Process a large CSV file of total return index data and persist it in the database.

    :param filename: Path to the CSV file
    :param as_of_date: The as-of date for the data
    :param sec_type: The security type (OEF, CEF, ETF, MMF)
    :param table_name: The name of the table to insert data into
    """
    chunk_size = 50000  # Adjust this based on your system's memory capacity
    total_rows = sum(1 for _ in open(filename, 'r')) - 1  # Count total rows, subtract 1 for header
    processed_rows = 0
    start_time = time.time()

    try:
        conn = get_connection()
        cursor = conn.cursor()

        with open(filename, 'r') as csvfile:
            reader = csv.reader(csvfile)
            next(reader)  # Skip header row

            chunk = [ ]
            for row in reader:
                mstar_id, fund_name, date, value, _ = row
                chunk.append((date, float(value), fund_name, mstar_id, as_of_date, sec_type))

                if len(chunk) == chunk_size:
                    insert_chunk(cursor, chunk, table_name)
                    processed_rows += len(chunk)
                    chunk = [ ]

                    # Print progress and time estimate
                    progress = processed_rows / total_rows
                    elapsed_time = time.time() - start_time
                    estimated_total_time = elapsed_time / progress if progress > 0 else 0
                    remaining_time = estimated_total_time - elapsed_time

                    print(f"Progress: {progress:.2%} | "
                          f"Estimated time remaining: {remaining_time / 60:.2f} minutes")

            # Insert any remaining rows
            if chunk:
                insert_chunk(cursor, chunk, table_name)
                processed_rows += len(chunk)

        conn.commit()
        print(f"Completed processing {processed_rows} rows in {(time.time() - start_time) / 60:.2f} minutes")

    except Exception as e:
        print(f"An error occurred: {str(e)}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()


def insert_chunk(cursor, chunk, table_name):
    """
    Insert a chunk of data into the database.
    """
    insert_query = f"""
    INSERT INTO {table_name} (date, value, fund_name, mstar_id, as_of_date, security_type)
    VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT (mstar_id, date, as_of_date) DO UPDATE
    SET value = EXCLUDED.value, fund_name = EXCLUDED.fund_name, security_type = EXCLUDED.security_type
    """
    cursor.executemany(insert_query, chunk)


def fetch_fund_symbols(as_of_date, security_types=['ETF', 'MMF', 'OEF', 'CEF']):
    with get_connection() as conn:
        with conn.cursor() as cur:
            query = """
                WITH latest_date AS (
                    SELECT MAX(as_of_date) as max_date
                    FROM security_master
                    WHERE security_type = ANY(%s)
                      AND as_of_date <= %s
                )
                SELECT symbol, mstar_id, security_type, as_of_date
                FROM security_master
                WHERE security_type = ANY(%s)
                  AND as_of_date = (SELECT max_date FROM latest_date)
            """
            cur.execute(query, (security_types, as_of_date, security_types))
            results = cur.fetchall()
            
            if not results:
                print(f"No data found for the specified security types up to {as_of_date}")
                return {}
            
            actual_as_of_date = results[0]['as_of_date']
            if actual_as_of_date != as_of_date:
                print(f"Using data as of {actual_as_of_date} (requested: {as_of_date})")
            
            return {row['symbol']: {'mstar_id': row['mstar_id'], 'security_type': row['security_type']} for row in results}

def process_fund_return_data(df):
    # Filter to market trading days
    nyse = mcal.get_calendar('NYSE')
    market_days = nyse.valid_days(start_date=df['date'].min(), end_date=df['date'].max())
    market_days = [mkt_date.date() for mkt_date in market_days]
    df = df.loc[df['date'].isin(market_days)].sort_values(['mstar_id', 'date'], ascending=[True, True])

    # Calculate returns
    df['return'] = df.groupby('mstar_id')['value'].pct_change()
    df.dropna(subset='return', inplace=True)

    return df

def calculate_fund_statistics_vectorized(mstar_ids, processed_data, as_of_date, symbol_mstar_map):
    # Group by mstar_id for calculations
    grouped = processed_data.groupby('mstar_id')
    
    # Calculate trading days
    trading_days = grouped['return'].size()
    
    # Calculate total return
    total_return = (1 + processed_data['return']).groupby(processed_data['mstar_id']).prod() - 1
    
    # Initialize annualized return and volatility series with NaN
    annualized_return = pd.Series(np.nan, index=trading_days.index)
    annualized_volatility = pd.Series(np.nan, index=trading_days.index)
    
    # Calculate annualized return and volatility only for funds with more than 30 trading days
    mask = trading_days > 30
    annualized_return[mask] = (1 + total_return[mask]) ** (252 / trading_days[mask]) - 1
    annualized_volatility[mask] = grouped['return'].std()[mask] * np.sqrt(252)
    
    # Combine results
    results = pd.DataFrame({
        'mstar_id': trading_days.index,
        'annualized_return': annualized_return,
        'annualized_volatility': annualized_volatility,
        'as_of_date': as_of_date,
        'trading_days': trading_days
    }).reset_index(drop=True)
    
    # Map mstar_id to symbol and security_type
    mstar_id_to_symbol = {v['mstar_id']: k for k, v in symbol_mstar_map.items()}
    results['symbol'] = results['mstar_id'].map(mstar_id_to_symbol)
    results['security_type'] = results['mstar_id'].map(lambda x: symbol_mstar_map[mstar_id_to_symbol[x]]['security_type'])
    
    print(f"Processed {len(results)} funds")
    
    return results

def process_chunk(chunk, as_of_date, symbol_mstar_map):
    try:
        # 1. Fetch return data for the chunk
        returns_data = fetch_fund_data(chunk, 'funds_total_return_index', as_of_date)
        
        # 2. Check that fetched return data is valid
        if isinstance(returns_data, list) and returns_data and isinstance(returns_data[0], dict) and 'error' in returns_data[0]:
            print(f"Error fetching return data for chunk: {returns_data[0]['error']}")
            return None
        
        # 3. Convert fetched return data to dataframe
        returns_df = pd.DataFrame(returns_data)
        
        # 4. Process the dataframe return data
        processed_data = process_fund_return_data(returns_df)
        
        # 5. Calculate fund statistics vectorized on this dataframe
        results = calculate_fund_statistics_vectorized(chunk, processed_data, as_of_date, symbol_mstar_map)
        
        # 6. Insert the processed chunk into the historical_return_statistics table
        insert_historical_return_statistics(results)
        
        return results
    except Exception as e:
        print(f"Error processing chunk: {str(e)}")
        return None

def calculate_fund_annualized_statistics(as_of_date, num_workers):
    # Fetch fund symbols and their mstar_ids
    symbol_mstar_map = fetch_fund_symbols(as_of_date)
    mstar_ids = [info['mstar_id'] for info in symbol_mstar_map.values()]
    
    # Split mstar_ids into chunks for parallel processing
    chunk_size = 100  # Adjust this value based on your needs
    mstar_id_chunks = [mstar_ids[i:i + chunk_size] for i in range(0, len(mstar_ids), chunk_size)]
    
    # Use parallel processing to process each chunk
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
        process_chunk_partial = partial(process_chunk, as_of_date=as_of_date, symbol_mstar_map=symbol_mstar_map)
        futures = [executor.submit(process_chunk_partial, chunk) for chunk in mstar_id_chunks]
        
        results = []
        for future in tqdm(concurrent.futures.as_completed(futures), total=len(mstar_id_chunks), desc="Processing chunks"):
            chunk_result = future.result()
            if chunk_result is not None:
                results.append(chunk_result)
    
    # Combine results from all chunks
    if results:
        results_df = pd.concat(results, ignore_index=True)
        print(f"Calculated and inserted annualized statistics for {len(results_df)} fund securities.")
    else:
        print("No valid results were produced.")

# Modify the insert_historical_return_statistics function to handle smaller chunks
def insert_historical_return_statistics(results_df):
    with get_connection() as conn:
        with conn.cursor() as cur:
            for _, row in results_df.iterrows():
                cur.execute("""
                    INSERT INTO historical_return_statistics 
                    (symbol, security_type, annualized_return, annualized_volatility, as_of_date, num_trading_days)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (symbol, security_type, as_of_date) 
                    DO UPDATE SET 
                        annualized_return = EXCLUDED.annualized_return,
                        annualized_volatility = EXCLUDED.annualized_volatility,
                        num_trading_days = EXCLUDED.num_trading_days,
                        created_at = CURRENT_TIMESTAMP
                """, (
                    row['symbol'],
                    row['security_type'],
                    row['annualized_return'],
                    row['annualized_volatility'],
                    row['as_of_date'],
                    row['trading_days']
                ))
            conn.commit()
    
    print(f"Inserted annualized statistics for {len(results_df)} fund securities.")

# Add this to your main function or create a new function to run the fund statistics calculation
def calculate_fund_stats(as_of_date, num_workers):
    start_time = time.time()
    calculate_fund_annualized_statistics(as_of_date, num_workers)
    end_time = time.time()
    duration = end_time - start_time
    log_to_csv('Fund Annualized Statistics', duration)
    logging.info(f"Fund annualized statistics calculation completed in {duration:.2f} seconds")

# You can add this to your main function
# calculate_fund_stats('2024-09-10', 25)
etf_df = pd.read_csv('/Users/gokul/Dropbox/Mac/Documents/Personal/Prosper/prosper_app/src/datas/data_09_15_2024/raw/etf_universe_characteristics20240916.csv')
characteristics_processing(etf_df, '2024-09-15', 'ETF', 'market_price_and_cap', 'funds_market_price_and_capitalization')

mmf_df = pd.read_csv('/Users/gokul/Dropbox/Mac/Documents/Personal/Prosper/prosper_app/src/datas/data_09_15_2024/raw/MMF_Characteristics20240916.csv')
characteristics_processing(mmf_df, '2024-09-15', 'MMF', 'market_price_and_cap', 'funds_market_price_and_capitalization')

oef_df = pd.read_csv('/Users/gokul/Dropbox/Mac/Documents/Personal/Prosper/prosper_app/src/datas/data_09_15_2024/raw/OE_MF_Characteristics20240916.csv')
characteristics_processing(oef_df, '2024-09-15', 'OEF', 'market_price_and_cap', 'funds_market_price_and_capitalization')

cef_df = pd.read_csv('/Users/gokul/Dropbox/Mac/Documents/Personal/Prosper/prosper_app/src/datas/data_09_15_2024/raw/CE_MF_Characteristics20240916.csv')
characteristics_processing(cef_df, '2024-09-15', 'CEF', 'market_price_and_cap', 'funds_market_price_and_capitalization')

