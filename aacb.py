#!/bin/env python

from datetime import date, timedelta, datetime

import lib.trades_data.cal_trades_data as pos_lim_cal
from lib.utils.utils import parse_arg, daterange
from lib.log import file_log as logging
from lib.db.db_connectors import DB_Connector as db_conn
import lib.gx_pos.aacb_hc as aacb_hc
import lib.gx_pos.gx_pos_data as gx_cur_pos
import pandas as pd
from datetime import timedelta
import lib.db.db_queries as dbq
from lib.utils.utils import printdf

import lib.liqreq.liqreq as liqreq
import lib.capreq.capreq as capreq
import lib.trades_data.cal_trades_data
from lib.positions.positions import get_positions


def main():
    global N
    date_T1 = '2023-11-14'
    N = 1
    args = parse_arg()
    logger = logging.FileLog.get_logger(args.debug, args.quiet_mode, __name__)
    logger.info(f'Invocation arguments: {args}')

    gx_cnx = db_conn.get_gx_cnx()
    sfi_cnx = db_conn.get_sfi_cnx()

    bf_cnx = db_conn.get_bankingfile_cnx()
    otdb_cnx = db_conn.get_otdb_cnx()

    #######################


    #######################
    # date_T1  = date.today()
    ### override for PyCharm testing - from CLI there are arguments
    date_T1 = datetime.strptime(date_T1, '%Y-%m-%d').date() # last day of Q3
    ###
    #######################
    date_TN = date_T1  + timedelta(days=-N)
    if args.startdate:
        date_T1  = datetime.strptime(args.startdate, '%Y-%m-%d').date()
        date_T1 = date_T1
        if args.stopdate:
            date_TN = datetime.strptime(args.stopdate, '%Y-%m-%d').date() + timedelta(days=-N)
    logger.info(f"date T  {date_T1 } to date T-N {date_TN} (exclusive)")

    i = 1
    for single_date in daterange(date_TN , date_T1):
        print(f'iteration {i}')
        i += 1
        # only process weekdays
        if single_date.weekday() < 5:
            logger.info(f"running process_date() for {single_date} (weekday={single_date.weekday()})")
            df_intratrades = pd.read_sql(dbq.get_intratrades_qry(single_date), gx_cnx)
            # df_intratrades = pd.read_sql(dbq.get_aacb_intratrades_qry(single_date), gx_cnx,bf_cnx,)

            printdf(df_intratrades.head(20))
            aacb_hc.get_hc_data(bf_cnx,sfi_cnx, single_date)
            gx_cur_pos.get_cur_pos(single_date, sfi_cnx, gx_cnx, bf_cnx,otdb_cnx)
            # aacb_hc.get_fx_rates(sfi_cnx)
            # positions_df = get_positions(query_date, sfi_cnx, gx_cnx, bf_cnx, otdb_cnx)
            #
            # capital_requirement = capreq.process_capreq(positions_df)

            pos_lim_cal.cal_trades_data(date_T1, date_TN, gx_cnx)

            # liquidty_requirement = liqreq.process_liqreq(positions_df)
            # logger.info(f"Liquidity Requirement met: {liquidty_requirement}")

        else:
            logger.debug(f"{single_date} is a weekend day (weekday={single_date.weekday()})")


if __name__ == '__main__':
    main()