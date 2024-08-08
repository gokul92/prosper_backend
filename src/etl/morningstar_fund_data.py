import os
import re
from typing import Dict, Union
from datetime import datetime
import numpy as np
import pandas as pd
from dotenv import load_dotenv
from morningstar_equity_data import add_to_db, get_connection

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
# TODO - Add total return info for ETFs, MFs and Funds
# LATER - Add function to read raw files from ftp and save in S3
# LATER - Create a droplet in digital ocean to parse the files and save in postgres tables
# LATER - Receive reminder when you have to buy back the primary security
# LATER - Marketing - What's a good way to set creatives for a loss harvesting product?
"""
yields_and_expenses_cols = [ 'NetExpenseRatio', 'SECYield', 'SECYieldDate',
                             'CategoryName', 'CategoryNetExpenseRatio', 'Yield1Yr', 'Yield1YrDate' ]

yields_and_expenses_numeric_cols = [ 'NetExpenseRatio', 'SECYield', 'CategoryNetExpenseRatio', 'Yield1Yr' ]

market_price_and_cap_cols = [ 'DayEndMarketPrice', 'DayEndMarketPriceDate', 'SharesOutstanding',
                              'SharesOutstandingDate' ]

market_price_and_cap_numeric_cols = [ 'DayEndMarketPrice', 'MarketCapital', 'SharesOutstanding' ]

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


def fetch_fund_data(ticker: str, table_name: str, as_of_date: Union[ str, datetime, None ] = None) -> Dict:
    """
    Fetch fund data for a given ticker, table, and date.

    :param ticker: The ticker symbol of the fund
    :param table_name: The name of the table to fetch data from
    :param as_of_date: The date for which to fetch data. Can be 'latest', a string in 'YYYY-MM-DD' format, or None (which defaults to 'latest')
    :return: A dictionary containing the fund data
    """
    conn = get_connection()
    cursor = conn.cursor()

    try:
        # Validate table name to prevent SQL injection
        valid_tables = [
            'funds_asset_allocation', 'funds_bond_sector', 'funds_categories',
            'funds_market_price_and_capitalization', 'funds_stock_sector',
            'funds_super_sector', 'funds_us_non_us', 'funds_yields_and_expenses'
        ]
        if table_name not in valid_tables:
            return {"error": f"Invalid table name: {table_name}"}

        if as_of_date is None or as_of_date == 'latest':
            query = f"""
            SELECT * FROM {table_name}
            WHERE ticker = %s
            ORDER BY as_of_date DESC
            LIMIT 1
            """
            cursor.execute(query, (ticker,))
        else:
            if isinstance(as_of_date, str):
                as_of_date = datetime.strptime(as_of_date, '%Y-%m-%d').date()

            query = f"""
            SELECT * FROM {table_name}
            WHERE ticker = %s AND as_of_date <= %s
            ORDER BY as_of_date DESC
            LIMIT 1
            """
            cursor.execute(query, (ticker, as_of_date))

        result = cursor.fetchone()

        if result is None:
            return {"error": f"No data found for ticker {ticker} in table {table_name}"}

        return dict(result)

    except Exception as e:
        return {"error": str(e)}
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
        'Yield1YrDate': 'yield_1_yr_date'
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
        processed_df = processed_df.dropna(subset=market_price_and_cap_cols, how='all')
        processed_df.loc[ processed_df[ 'DayEndMarketPriceDate' ].isin([ '', ' ' ]), 'DayEndMarketPriceDate' ] = np.nan
        processed_df[ 'DayEndMarketPriceDate' ].fillna(processed_df[ 'PortfolioDate' ], inplace=True)
        processed_df[ 'MarketCapitalization' ] = processed_df[ 'DayEndMarketPrice' ] * processed_df[
            'SharesOutstanding' ]
        processed_df.dropna(subset='MarketCapitalization', inplace=True)

    if 'processed_df' in locals():
        renamed_col_names = {}
        for col in processed_df.columns:
            renamed_col_names[ col ] = to_snake_case(col)

        processed_df[ 'as_of_date' ] = as_of_date

        if sec_type in [ 'OEF', 'CEF', 'ETF', 'MMF' ]:
            processed_df[ 'security_type' ] = sec_type

        processed_df = processed_df.rename(columns=renamed_col_names)

        add_to_db(processed_df, table_name)

# etf_char_df = pd.read_csv(
#     '/Users/gokul/Documents/Personal/Prosper/prosper_app/src/datas/data_08_07_2024/raw/characteristics/etf_universe_characteristics20240807_gramanathan.csv')
# df = raw_df_processing(etf_char_df)
# characteristics_processing(df, '2024-08-01', 'ETF', 'market_price_and_cap', 'fund_market_price_and_capitalization')
# #
# oef_char_df = pd.read_csv(
#     '/Users/gokul/Documents/Personal/Prosper/prosper_app/src/datas/data_08_07_2024/raw/characteristics/OE_MF_Characteristics20240807_gramanathan.csv')
# df = raw_df_processing(oef_char_df)
# characteristics_processing(df, '2024-08-01', 'OEF', 'market_price_and_cap', 'fund_market_price_and_capitalization')
# #
# cef_char_df = pd.read_csv(
#     '/Users/gokul/Documents/Personal/Prosper/prosper_app/src/datas/data_08_07_2024/raw/characteristics/CE_MF_Characteristics20240807_gramanathan.csv')
# df = raw_df_processing(cef_char_df)
# characteristics_processing(df, '2024-08-01', 'CEF', 'market_price_and_cap', 'fund_market_price_and_capitalization')
#
# mmf_char_df = pd.read_csv(
#     '/Users/gokul/Documents/Personal/Prosper/prosper_app/src/datas/data_08_07_2024/raw/characteristics/MMF_Characteristics20240807_gramanathan.csv')
# df = raw_df_processing(mmf_char_df)
# characteristics_processing(df, '2024-08-01', 'MMF', 'market_price_and_cap', 'fund_market_price_and_capitalization')
