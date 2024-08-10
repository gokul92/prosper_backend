import pandas as pd

from morningstar_equity_data import add_to_db, get_symbol_guide


def add_to_security_master(as_of_date, source_fn, sec_type):
    sec_master_table = 'security_master'
    if sec_type == 'Equity':
        equity_master_cols = [ 'H1', 'S12', 'S1012' ]
        master_df = pd.DataFrame(get_symbol_guide())
        master_df = master_df[ equity_master_cols ]
        master_df = master_df.loc[ ~master_df[ 'H1' ].isnull() ]
        master_df = master_df.loc[ ~master_df[ 'H1' ].isin([ '', ' ' ]) ]
        master_df.columns = [ 'symbol', 'security_name', 'cusip' ]
        master_df[ 'security_type' ] = sec_type
        master_df[ 'as_of_date' ] = as_of_date
        add_to_db(master_df, sec_master_table)
        return True
    elif sec_type in [ 'ETF', 'OEF', 'CEF', 'MMF' ]:
        master_df = pd.read_csv(source_fn)
        cols_list = [ 'MStarID', 'Ticker', 'CUSIP', 'FundName' ]
        master_df = master_df[ cols_list ]
        master_df = master_df.loc[ ~master_df[ 'Ticker' ].isnull() ]
        master_df = master_df.loc[ ~master_df[ 'Ticker' ].isin([ '', ' ' ]) ]
        master_df.columns = [ 'mstar_id', 'symbol', 'cusip', 'security_name' ]
        master_df[ 'security_type' ] = sec_type
        master_df[ 'as_of_date' ] = as_of_date
        add_to_db(master_df, sec_master_table)
        return True
    else:
        return False


# source_dir = '/Users/gokul/Dropbox/Mac/Documents/Personal/Prosper/prosper_app/src/datas/data_08_10_2024/raw/characteristics/'

# result_ce = add_to_security_master('2024-08-10', source_dir + 'CE_MF_Characteristics20240810.csv', 'CEF')
# print(f'CE sec master ingestion status: {result_ce}')

# result_etf = add_to_security_master('2024-08-10', source_dir + 'etf_universe_characteristics20240810.csv', 'ETF')
# print(f'ETF sec master ingestion status: {result_etf}')

# result_mmf = add_to_security_master('2024-08-10', source_dir + 'MMF_Characteristics20240810.csv', 'MMF')
# print(f'MMF sec master ingestion status: {result_mmf}')

# result_oef = add_to_security_master('2024-08-10', source_dir + 'OE_MF_Characteristics20240810.csv', 'OEF')
# print(f'OEF sec master ingestion status: {result_oef}')

# result_equity = add_to_security_master('2024-08-10', '', 'Equity')
# print(f'Equity sec master ingestion status: {result_equity}')