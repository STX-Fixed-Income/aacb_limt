import pandas as pd
from lib.log import file_log as logging
import lib.db.db_queries as dbq
from lib.utils.utils import printdf
import numpy as np
logger = logging.FileLog.get_logger(name='positions')


def get_positions(query_date, sfi_cnx, gx_cnx, bf_cnx,otdb_cnx):
    # get fx rate and calculate market value of position in euros
    logger.info(f"Getting real-time positions as of {query_date} from GenXs tradingsystems database")
    gx_df = pd.read_sql(dbq.get_genxs_postions_qry(query_date), gx_cnx)
    gx_df.rename(columns=str.lower, inplace=True)
    logger.debug(f"\n{gx_df.head()}")

    logger.info(f"Getting ratings, categories, other metadata as of {query_date} from sfi_transaction.bloomberg_risk table")
    bonddata_df = pd.read_sql(dbq.get_bbg_bond_data_qry(query_date), sfi_cnx)
    bonddata_df.rename(columns=str.lower, inplace=True)
    logger.debug(f"\n{bonddata_df.head()}")

    logger.info(f"Getting most recent own-prices as of {query_date} from bankingfiles.bbg_positions")
    bidask_df = pd.read_sql(dbq.get_our_prices_qry(query_date), bf_cnx)
    bidask_df.rename(columns=str.lower, inplace=True)
    printdf(bidask_df.head(2))
    print(bidask_df[bidask_df['isin'] =='XS2741415223'])
    print(bidask_df['isin'] =='XS2741415223')
    print(bidask_df[bidask_df['isin'] =='XS2741415223'])

    logger.debug(f"\n{bidask_df.head()}")

    pd.set_option('display.max_columns', None)

    logger.info(f"Setting market value in Euros using SFI's prices")
    for idx, row in gx_df.iterrows():
        isin= row['isin']
        # We need a non-zero bid_price, variable value will persist the iterations so reset it for display purposes
        try:
            bid_price = None
            bid_price = bidask_df.loc[bidask_df['isin'] == isin, ['best_bid']].values[0][0]
            # ask_price = bidask_df.loc[bidask_df['isin'] == isin, ['best_ask']].values[0][0]
            if bid_price <= 0 or bid_price is None:
                raise Exception
        except Exception as e:
            last_paid = get_last_paid(isin, logger, gx_cnx)
            logger.info(f"No bid_price for {isin} (bp:{bid_price}). Using last paid for bid_price: {last_paid}")
            bid_price = last_paid

        gx_df.at[idx, 'marketvalue_eur'] = row["current_position"] * bid_price * row["fx_rate"]
    logger.info(f"Finished setting market value in Euros.")


    logger.info(f"Merge GenXs's {gx_df.shape[0]} positions with BBG bond data ({bonddata_df.shape[0]} rows).")
    df = gx_df.merge(bonddata_df, how='left', on='isin', suffixes=(None, '_bbg'))
    if df.shape[0] != gx_df.shape[0]:
        logger.info(f"Pre-merge: {gx_df.shape[0]} positions (row) and {gx_df.shape[1]} columns")
        logger.error(f"Post-merge positions changed: {df.shape[0]} positions (row) and {df.shape[1]} columns.")
    logger.debug(f"\n{df.head()}")
    logger.info(f"Finished merge. Result {df.shape[0]} positions with ({df.shape[0]} rows).")


    missing_data_df = df.loc[df['isin'].isnull()]
    missing_data_df.reset_index(inplace=True)
    logger.info(f"Finding GenXs positions missing BBG bond data. {missing_data_df.shape[0]} rows found.")
    for idx, row in missing_data_df.iterrows():
        if row['current_position'] != 0:
            logger.warning(f"Missing BBG bond data for {row['isin']} with position: {row['current_position']}")
            metadata = get_missing_metadata(row['isin'])
        else:
            logger.info(f"Row {idx + 1}: Ignore missing data for {row['isin']}, position: {row['current_position']}")

    # sys.exit(1)

    return df


def get_last_paid(isin, logger, cnx):

    price_df = pd.read_sql(dbq.get_last_paid_qry(isin), cnx)

    if price_df.shape[0] < 1:
        logger.info(f"There is no natural last paid for {isin}. Get simtrade last paid.")
        price_df = pd.read_sql(dbq.get_simtrade_last_paid_qry(isin), cnx)

    try:
        price = price_df.iloc[-1:, 0].values[0]
    except Exception as err:
        logger.error(f"There is no last paid for {isin}. Investigation required.")
        raise err

    return price

def get_missing_metadata(isin):
    # TODO: retrieve metadata when missing or exclude ISINs missing metadata
    logger.error("TODO: retrieve metadata when missing or exclude ISINs missing metadata")

