import pandas as pd
from datetime import timedelta
import lib.db.db_queries as dbq
from lib.log import file_log as logging
from lib.utils.utils import printdf
from datetime import datetime

def pos_lim_forec(date_T, gx_cnx):

    logger = logging.FileLog.get_logger(name='__current_real_position__')
    logger.info("Running real time position limit forecasting")

    df_trades_today = get_trades(date_T, gx_cnx, logger)
    cash_baln_today = cash_baln_day_T(df_trades_today, date_T)
    # df_cur_pos = get_cur_pos(date_T, gx_cnx)
    printdf(df_trades_today.head(10))
    print(cash_baln_today)
    # print(df_cur_pos.columns)

    # printdf(df_cur_pos.head(20))
    return True


def get_trades(date_T, gx_cnx, logger):

    end_date = date_T + timedelta(days=-1)
    logger.info(f"Getting the current trades from day T-1: {str(end_date)} ")

    df = pd.read_sql(dbq.get_trades_qry(date_T, end_date), gx_cnx)
    print(end_date , date_T)
    df.rename(columns=str.lower, inplace=True)
    return df
    # printdf(df)

def cash_baln_day_T(df, date_T):
    # Filter based on conditions

    df['settledate'] = pd.to_datetime(df['settledate'])

    # Define query_date
    date_T = pd.Timestamp(date_T)
    settled_df = df[(df['settledate'] == date_T) & (df['contracttypename'] == 'Bond') & (df['storno'] == 0)]
    unsettled_df = df[(df['settledate'] > date_T) & (df['contracttypename'] == 'Bond') & (df['storno'] == 0)]

    date_T1 = date_T + timedelta(days=-1)
    unsettled_df2 = df[(df['tradedate'] == date_T1) & (df['settledate'] == date_T) & (df['contracttypename'] == 'Bond') & (df['storno'] == 0)]

    settled_cash = cal_cash_bln(settled_df)
    unsettled_cash = cal_cash_bln(unsettled_df)
    unsettled_cash2 = cal_cash_bln(unsettled_df2)

    # Calculate the sums for 'sell' and 'buy'
    # sell_sum = settled_df[settled_df['side'] == 'sell']['total_value_in_eur'].sum()
    # buy_sum = settled_df[settled_df['side'] == 'buy']['total_value_in_eur'].sum()
    #
    # # Calculate the net total value
    # net_total_value =buy_sum - sell_sum

    print("Net Total Value in EUR for Bond Transactions: ", settled_cash,unsettled_cash,unsettled_cash2)

def cal_cash_bln(df):
    sell_sum = df[df['side'] == 'sell']['total_value_in_eur'].sum()
    buy_sum = df[df['side'] == 'buy']['total_value_in_eur'].sum()

    # Calculate the net total value
    return buy_sum - sell_sum
def get_cur_pos(date_T, gx_cnx):

    logger = logging.FileLog.get_logger(name='positions')
    # get fx rate and calculate market value of position in euros
    logger.info(f"Getting real-time positions as of {date_T} from GenXs tradingsystems database")
    gx_df = pd.read_sql(dbq.get_genxs_postions_qry(date_T), gx_cnx)
    gx_df.rename(columns=str.lower, inplace=True)
    logger.debug(f"\n{gx_df.head()}")
    return gx_df
    # printdf(gx_df.head())