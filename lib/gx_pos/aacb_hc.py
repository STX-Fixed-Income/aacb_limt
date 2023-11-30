import pandas as pd
import numpy as np
from datetime import timedelta
import lib.db.db_queries as dbq
from lib.log import file_log as logging
from lib.utils.utils import printdf
N = 1
def get_hc_data(bf_cnx, sfi_cnx, day_T):
    logger = logging.FileLog.get_logger(name='__fccmdef__')

    fccmdef = pd.read_sql(dbq.get_aacb_fccmdef_qry(), bf_cnx)
    liqcdef = pd.read_sql(dbq.get_aacb_liqcdef_qry(), bf_cnx)

    fccm_df = fccmdef[fccmdef['file_date'].dt.date == day_T]
    liqc_df = liqcdef[liqcdef['file_date'].dt.date == day_T]

    cash_baln_T = get_cash_baln(bf_cnx, sfi_cnx, day_T)
    print(f'the aacb cash pay rec on date {day_T} and computed based on data are xxxxxxx and {cash_baln_T}')

    print(f'the aacb cash balance on date {day_T} and computed based on data are xxxxxx and {cash_baln_T}')

    for i in range(1,N+1):
        cash_baln_Tn_ = get_cash_baln(bf_cnx, sfi_cnx, (day_T + timedelta(days=-i)))

        # print(f'the aacb cash balance on date {day_T} and computed based on data are xxxxxx and {cash_baln_TN_}')
        print(f'the aacb cash balance on date {day_T + timedelta(days=-i)} and computed based on data are xxxxxx and {cash_baln_Tn_}')

        # print(f'the aacb cash pay rec on date {day_T} and computed based on data are xxxxxxx and {cash_baln_TN_}')
        print(f'the aacb cash pay rec on date {day_T + timedelta(days=-i)} and computed based on data are xxxxxxx and {cash_baln_Tn_}')


    # cash_balance_T1 = -30000881
    # logger.info(f"The cash balance AACB on {day_T} is {round(cash_balance_T1, 2)}")

    inelig_fin = cal_inelig_fin(fccm_df, cash_baln_T, logger)
    logger.info(f"On {day_T} the Ineligibility fin AACB is {round(inelig_fin, 2)}")

    return True

def cal_inelig_fin(fccm_df, cash_baln,logger):
    cash_baln_t = -30015163.11
    baln_pay_rec = 5463828.97
    mtm_net = 0
    # TODO import the cash_balance and baln_pay_rec from FCCMDEF file at day T @Dan
    # TODO import the mtm_net from SACCRDEF file at day T @Dan


    eligible_col = (fccm_df[fccm_df['index'] == 'N']['eligible_col']).sum()
    short_stock = fccm_df['mtm_short_bcy'].sum()

    credit_util = -short_stock - cash_baln - baln_pay_rec - np.min(mtm_net, 0)
    inelig_fin = np.max(credit_util-eligible_col)

    logger.info(f"Discrepancy of the cash_balance is {round(cash_baln_t - cash_baln, 2)}")

    # print(credit_util, eligible_col, inelig_fin)
    # print(f"Discrepancy of the cash_balance is {round(cash_balance_t - cash_balance, 2)}")
    return inelig_fin

def get_cash_baln(bf_cnx, sfi_cnx, day_T1):
    # data source: bankingfiles.interestclientlevel
    df_cash_bln = pd.read_sql(dbq.get_aacb_cash_balance(), bf_cnx)

    # get the cash balance at day T with the data from T-1
    df_cash_bln = df_cash_bln[(df_cash_bln['valuedate'].dt.date == day_T1) & (df_cash_bln['processingdate'].dt.date == day_T1)]
    # printdf(df_cash_bln.head(20))
    fx_mapping_dict = get_fx_rates(sfi_cnx)

    df= df_cash_bln.copy()
    df['value_in_eur'] = df.apply(
        lambda row: row['grossbalanceamount_value'] * get_single_rate(row['grossbalanceamount_valuecur'], fx_mapping_dict)
        if row['grossbalanceamount_valuecur'] != 'EUR'
        else row['grossbalanceamount_value'], axis=1)

    # Sum up the values when 'grossbalanceamount_valuedc' is 'C: credit'
    sum_c = df['value_in_eur'][np.where(df['grossbalanceamount_valuedc'] == 'C', True, False)].sum()
    # Sum up the negative values when 'grossbalanceamount_valuedc' is 'D: debit'
    sum_d = df['value_in_eur'][np.where(df['grossbalanceamount_valuedc'] == 'D', True, False)].sum()

    cash_baln = round(sum_c-sum_d,2)

    return cash_baln

def get_fx_rates(sfi_cnx):

   df_fx = pd. read_sql(dbq.get_sfi_txs_fx(), sfi_cnx)

   # # Sort by date and drop duplicates to keep only the most recent rate for each currency pair
   fx_sorted = df_fx.sort_values(by='rate_date', ascending=False)
   fx_mapping_df = fx_sorted.drop_duplicates(subset=['base_currency', 'quote_currency'])

   fx_mapping_dict = pd.Series(fx_mapping_df.rate.values, index=fx_mapping_df.quote_currency).to_dict()

   return fx_mapping_dict

def get_single_rate(currency, fx_mapping_dict):

    # Default to 1 if EUR or not found
   return fx_mapping_dict.get(currency, 1)

