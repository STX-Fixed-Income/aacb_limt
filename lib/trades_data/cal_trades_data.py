import pandas as pd
from datetime import timedelta
import lib.db.db_queries as dbq
from lib.log import file_log as logging
from lib.utils.utils import printdf
from datetime import datetime

# N = 1
def cal_trades_data(date_T, date_TN, gx_cnx):

    logger = logging.FileLog.get_logger(name='__current_real_position__')
    logger.info("Running real time position limit forecasting")

    df_trades_ndays = get_trades(date_T, date_TN, gx_cnx, logger)
    printdf(df_trades_ndays.head(5))
    print(cal_intraday_mtm(df_trades_ndays.copy(), date_TN))


    # cash_baln_today = cash_baln_day_T(df_trades_ndays, date_T)
    # df_cur_pos = get_cur_pos(date_T, gx_cnx)
    # # printdf(df_trades_ndays.head(10))
    # # print(cash_baln_today)
    # print(df_trades_ndays.columns)
    # print(df_trades_ndays['marketid'].unique())
    # print(df_trades_ndays['tradedate'].unique())
    # print(df_trades_ndays['settledate'].unique())
    # print(df_trades_ndays['contracttypename'].unique())

    printdf(df_trades_ndays.head(5))
    return True


def get_trades(date_T, date_TN, gx_cnx, logger):
    # global N
    # end_date = date_T + timedelta(days=-N)
    logger.info(f"Getting the current trades from the past N days: {str(date_TN)} ")
    print(date_T,date_TN)
    df = pd.read_sql(dbq.get_trades_qry(date_T, date_TN), gx_cnx)
    # print(date_TN, date_T)
    df.rename(columns=str.lower, inplace=True)
    return df
    # printdf(df)


def cal_intraday_mtm(df, date_T):
    df = df[df['contracttypename'] != 'Future']

    date_T = datetime(year=date_T.year, month=date_T.month, day=date_T.day)

    sell_sum = df[(df['side'] == 'sell') & (df['tradedate'] == date_T)]['total_value_in_eur'].sum()
    buy_sum = df[(df['side'] == 'buy') & (df['tradedate'] == date_T)]['total_value_in_eur'].sum()

    # Calculate the net total mtm value
    return buy_sum - sell_sum
def cash_baln_day_T(df, date_T):
    # Filter based on conditions
    unique_trade_date = df['tradedate'].unique()
    unique_settle_date = df['settledate'].unique()

    print(unique_trade_date, unique_settle_date)
    df['settledate'] = pd.to_datetime(df['settledate'])

    date_T = pd.Timestamp(date_T)
    date_T1 = (date_T + timedelta(days=1))
    settled_df = df[(df['tradedate'] == date_T) & (df['settledate']== date_T1) & (df['storno'] == 0)]
    unsettled_df = df[(df['tradedate'] == date_T) & (df['settledate'] > date_T1) & (df['storno'] == 0)]

    # date_T1 = date_T + timedelta(days=-1)
    # settled_df_T1 = df[(df['settledate'] == date_T1) & (df['contracttypename'] == 'Bond') & (df['storno'] == 0)]
    # unsettled_df_T1 = df[(df['tradedate'] == date_T1) & (df['settledate'] == date_T) & (df['contracttypename'] == 'Bond') & (df['storno'] == 0)]

    settled_cash_T = cal_cash_bln(settled_df)
    unsettled_cash_T = cal_cash_bln(unsettled_df)
    # settled_cash_T1 = cal_cash_bln(settled_df_T1)
    # unsettled_cash_T1 = cal_cash_bln(unsettled_df_T1)

    # Calculate the sums for 'sell' and 'buy'
    # sell_sum = settled_df[settled_df['side'] == 'sell']['total_value_in_eur'].sum()
    # buy_sum = settled_df[settled_df['side'] == 'buy']['total_value_in_eur'].sum()
    #
    # # Calculate the net total value
    # net_total_value =buy_sum - sell_sum
    # print(f'The cash_baln change at day T-1 : {date_T1} is: {settled_cash_T1}')
    # print(f'The cash_pay_rec change at day T-1 : {date_T1} is: {-unsettled_cash_T1}')
    print(f'The cash_baln change at day T : {date_T} is: {settled_cash_T}')
    print(f'The cash_pay_rec change at day T: {date_T} is: {-unsettled_cash_T}')

    # print("Net Total Value in EUR for Bond Transactions: ", settled_cash_T,unsettled_cash_T,settled_cash_T1,unsettled_cash_T1)

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