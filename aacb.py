#!/bin/env python

from datetime import date, timedelta, datetime

import lib.trades_data.cal_trades_data as get_trades_data
from lib.utils.utils import parse_arg, daterange
from lib.log import file_log as logging
from lib.db.db_connectors import DB_Connector as db_conn
import lib.gx_pos.aacb_hc as aacb_hc
import lib.gx_pos.gx_pos_data as gx_cur_pos
import pandas as pd
from datetime import timedelta
import lib.db.db_queries as dbq
from lib.utils.utils import printdf
import os
from datetime import datetime


import lib.liqreq.liqreq as liqreq
import lib.capreq.capreq as capreq
import lib.trades_data.cal_trades_data
from lib.positions.positions import get_positions


def main():
    date_T = '2023-12-28'  # The date to do estimation / today / to be estimated
    # date_T  = date.today()
    date_T = datetime.strptime(date_T, '%Y-%m-%d').date()
    dates = get_dates(date_T)
    date_T1_, date_T2_ = dates
    print(date_T1_, date_T2_)

    args = parse_arg()
    logger = logging.FileLog.get_logger(args.debug, args.quiet_mode, __name__)
    logger.info(f'Invocation arguments: {args}')

    gx_cnx = db_conn.get_gx_cnx()
    sfi_cnx = db_conn.get_sfi_cnx()
    bf_cnx = db_conn.get_bankingfile_cnx()
    otdb_cnx = db_conn.get_otdb_cnx()

    #         # limit check for day T
    #         # logger.info(f"limit checking based on current positions for {single_date} (weekday={single_date.weekday()})")
    #         # cal_limit(df_intratrades, df_fccmdef_day, hc_rep['ineligible_fin'].iloc[-1], estimation = False)
    #
    #         # limit estimation for day T+1
    #         logger.info(f"limit estimation based on current positions for {date_T} (weekday={date_T.weekday()})")
    #         cal_limit(df_intratrades_T1, df_fccmdef_all, hc_rep['ineligible_fin'].iloc[-1], abn_break_val, estimation = True)
    #
    #         # inelig_fin_UB = df_eligible[df_eligible['eligible'] == 'N']['abn_mtm_value'].sum()
    #         # print(inelig_fin_UB)

    #         # aacb_hc.get_hc_data(bf_cnx,sfi_cnx, single_date)
    #         # gx_cur_pos.get_cur_pos(single_date, sfi_cnx, gx_cnx, bf_cnx,otdb_cnx)

    #
    #         # aacb_hc.get_fx_rates(sfi_cnx)

    #
    #         # get_trades_data.cal_trades_data(date_T, date_T1, gx_cnx)

    #
    #     else:
    #         logger.debug(f"{single_date} is a weekend day (weekday={single_date.weekday()})")
        # only process weekdays
    # print(date_T.weekday())

    #         # logger.info(f"getting ineligible info for current positions for {date_T} (weekday={date_T.weekday()})")
    #         n = 1

    #
    #         # aacb_hc.get_hc_data(bf_cnx,sfi_cnx, single_date)
    #         # gx_cur_pos.get_cur_pos(single_date, sfi_cnx, gx_cnx, bf_cnx,otdb_cnx)
    #
    #         # aacb_hc.get_fx_rates(sfi_cnx)
    #         # positions_df = get_positions(query_date, sfi_cnx, gx_cnx, bf_cnx, otdb_cnx)
    #         #
    #         # capital_requirement = capreq.process_capreq(positions_df)
    #
    #         # get_trades_data.cal_trades_data(date_T, date_T1, gx_cnx)

    #         # logger.debug(f"{date_T} is a weekend day (weekday={date_T.weekday()})")


    limit_dataset = limit_process(date_T1_, date_T, gx_cnx, bf_cnx, logger)
    df_intratrades, df_fccmdef_day,df_fccmdef_all = limit_dataset
    df_pos = gx_cur_pos.get_cur_pos(date_T, sfi_cnx, gx_cnx, bf_cnx, otdb_cnx)
    df_pos = df_pos.copy()
    print(df_pos['marketvalue_eur'].sum())

    df_pos = df_pos[df_pos['contracttype'] == 'Bond']
    print(df_pos['marketvalue_eur'].sum())
    printdf(df_pos.head(5))
    abn_break_val_T_ = cal_abn_break_val(bf_cnx, date_T)
    abn_break_val_T1_ = cal_abn_break_val(bf_cnx, date_T1_)
    abn_break_val_T2_ = cal_abn_break_val(bf_cnx, date_T2_)

    hc_rep = fccm_summary(bf_cnx, date_T1_)

    #TODO  get our price for the 100 valuation prices
    # limit = []
    # print(cal_limit(df_intratrades, df_fccmdef_day, hc_rep['ineligible_fin'].iloc[-1], abn_break_val, estimation = True))
    # limit.append(cal_limit(df_intratrades, df_fccmdef_all, hc_rep['ineligible_fin'].iloc[-1], abn_break_val, estimation = True))
    # print(f'The limit interval is: ( {min(limit)}, {max(limit)})')
    #
    limit = []

    # Calculate and append the limits to the list
    limit.append(cal_limit(df_intratrades, df_fccmdef_day, hc_rep['ineligible_fin'].iloc[-1], abn_break_val_T_, estimation=True).iloc[0])
    limit.append(cal_limit(df_intratrades, df_fccmdef_all, hc_rep['ineligible_fin'].iloc[-1], abn_break_val_T_, estimation=True).iloc[0])

    ## check the limit
    # chk_lim(fccm_summary(bf_cnx, date_T2_)['ineligible_fin'].iloc[-1], abn_break_val_T_, date_T1_)

    # Check if the list is empty (no limits calculated)
    if not limit:
        print("No limits calculated.")
    else:
        # Print the interval with the minimum and maximum values
        print(f'The limit interval is: ( {min(limit)}, {max(limit)})')

def get_dates(date_T):

    # Determine date_T1_ based on the weekday of date_T
    if date_T.weekday() == 0:  # Monday
        date_T1_ = date_T + timedelta(days=-3)
        date_T2_ = date_T + timedelta(days=-4)

    elif date_T.weekday() == 6:  # Sunday
        date_T1_ = date_T + timedelta(days=-2)
    elif date_T.weekday() == 5:  # Saturday (5)
        date_T1_ = date_T + timedelta(days=-1)
    elif date_T.weekday() == 1:  # Tuesday
        date_T1_ = date_T + timedelta(days=-1)
        date_T2_ = date_T + timedelta(days=-4)
    else:
        date_T1_ = date_T + timedelta(days=-1)  # Default for other weekdays
        date_T2_ = date_T + timedelta(days=-2)
    return date_T1_, date_T2_

def limit_process(date_T1_, date_T, gx_cnx, bf_cnx, logger):
    df_intratrades = pd.read_sql(dbq.get_intratrades_qry(date_T), gx_cnx)
    df_fccmdef_day = pd.read_sql(dbq.get_fccmdef_day_qry(date_T1_), bf_cnx)
    df_fccmdef_all = pd.read_sql(dbq.get_fccmdef_all_qry(), bf_cnx)

    # limit check for day T  ##TODO
    # logger.info(f"limit checking based on current positions for {single_date} (weekday={single_date.weekday()})")
    # cal_limit(df_intratrades, df_fccmdef_day, hc_rep['ineligible_fin'].iloc[-1], estimation = False)

    # limit estimation for day T (to be reported on T+1)
    logger.info(f"limit estimation based on current positions for {date})")
    return df_intratrades, df_fccmdef_day, df_fccmdef_all

def fccm_summary(bf_cnx, date):
    df_fccmdef_summary = pd.read_sql(dbq.get_fccmdef_summary_qry(), bf_cnx)
    hc_rep = df_fccmdef_summary[df_fccmdef_summary['file_date'] == str(date)]
    hc_rep = hc_rep.copy()
    hc_rep['ineligible_fin'] = -(hc_rep['cash_exposure'] + hc_rep['eligible_collateral'] + hc_rep['short_stock'])
    printdf(f'the faccmdef file received on day T-1:= {date} is: {hc_rep}')
    return hc_rep

def cal_abn_break_val(bf_cnx, date):

    ##  calculates and returns the negative net value of abnormal breaks for a given date based on data retrieved from a abn settlement breaks table.
    df = pd.read_sql(dbq.get_abn_breaks_qry(), bf_cnx)
    df = df.copy()
    df['abn_eod_position'].fillna(0, inplace=True)
    df['genxs_position'].fillna(0, inplace=True)
    df['diff'] = df['abn_eod_position'] - df['genxs_position']
    # printdf(df[(df['break_date'] == date)])
    break_value = df[(df['break_date'] == date) & (df['abn_eligibility'] != 'Y') & (df['contracttype'] != 'Future')]['diff'].sum()
    print(f'abn breaks that noted on {date} is: {break_value}')
    return(-break_value)

def cal_limit(df_intratrades, df_fccmdef, inelig_lim_T1_, abn_break_val_T_, estimation):

    df = pd.merge(df_intratrades, df_fccmdef, on='isin', how='left')
    print('the new dataframe is')

    # Replace all NaN values in the valuation_price column with 100 and all NaN values in the eligible column with 'N'.
    df['valuation_price'] = df['valuation_price'].fillna(100)
    df['eligible'] = df['eligible'].fillna('N')
    # printdf(df[(df['filter_col'] == 1) & (df['eligible'] =='N')])

    if estimation:
        df['abn_mtm_value'] = df['valuation_price'] * df['Overnight'] * 0.01

    ## APPROACH ONE: cerrent (real-time) postion with aacb valuation price, eligibility condition and abn breaks
    ## NOTE: The eligibility is updated according to the historical fccmdef file, differing from eligibility grafana pagee

    sum_inelig = (df[df['eligible'] == 'N']['valuation_price'] *
                  df[df['eligible'] == 'N']['position'] * 0.01).sum()
    sum_elig = (df[df['eligible'] == 'Y']['valuation_price'] *
                df[df['eligible'] == 'Y']['position'] * 0.01).sum()
    delta_ineli_exp = df[df['eligible'] == 'N']['diff_exposure'].sum()

    delta_ineli_mtm = (df[df['eligible'] == 'N']['valuation_price'] *
                       df[df['eligible'] == 'N']['diff_exposure'] * df['contractsize']).sum()
    abn_overnight_mtm = df['abn_mtm_value'].sum()

    summary = pd.DataFrame({
        'Metric': ['Date_T','Run_at', 'Sum of elig_Position', 'Sum of Inelig_Position', 'ANB_MTM_Overnight',
                   'Ineligible_Exp_Change', 'Inelig_Expos_MTM (ABN)', 'Total_Pos',
                   'Total_Overnight_Pos', 'Total_Exp_Change', 'MTM_Exp_Change',
                   'ABN_MTM_Inelig_Change', 'Number_Inelig_Change',
                   'if_limit_t1_', 'Estimation'],
        'Value': [df['date'][0],datetime.now().strftime("%Y-%m-%d %H:%M:%S"), sum_elig,  sum_inelig, abn_overnight_mtm,
                  delta_ineli_exp, delta_ineli_mtm, df['position'].sum(),
                  df['Overnight'].sum(), df['diff_exposure'].sum(), (df['valuation_price'] * df['diff_exposure'] * df['contractsize']).sum(),
                  df[(df['diff_exposure'] != 0) & (df['eligible'] == 'N')]['abn_mtm_value'].sum(),
                  int(len(df[(df['filter_col'] == 1) & (df['eligible'] == 'N')])),
                  inelig_lim_T1_, (inelig_lim_T1_ + delta_ineli_mtm + abn_break_val_T_)]
    })

    print(summary)
    current_directory = os.getcwd()
    csv_file_name = 'summary_results.csv'
    csv_file_path = os.path.join(current_directory, csv_file_name)
    # Save the DataFrame to a CSV file, appending to existing file if it exists
    summary.to_csv(csv_file_path, index=False, mode='a', header=not os.path.exists(csv_file_path))

    print(f"Does the 'valuation_price' column contain NaN values? {df['valuation_price'].isna().any()}")
    return summary[summary['Metric'] == 'Estimation']['Value']

def chk_lim(limit_T2_, abn_break_val_T_, date_T1_):
    ## The date to be checked can only be T-1, not T!
    ## Return the check of T-1
    print(f'the limit on {date_T1_} forecasted without unexpected breaks is {limit_T2_ - abn_break_val_T_}')
    print(limit_T2_, abn_break_val_T_)
    return limit_T2_ - abn_break_val_T_

if __name__ == '__main__':
    main()