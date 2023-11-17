#!/bin/env python

from datetime import date, timedelta, datetime

import lib.real_time_lim_cal.real_time_pos as pos_lim_cal
from lib.utils.utils import parse_arg, daterange
from lib.log import file_log as logging
from lib.db.db_connectors import DB_Connector as db_conn
import lib.limit_forecast.aacb_hc as aacb_hc
import lib.liqreq.liqreq as liqreq
import lib.capreq.capreq as capreq
import lib.real_time_lim_cal.real_time_pos
from lib.positions.positions import get_positions


def main():

    date_T1 = '2023-11-02'
    args = parse_arg()
    logger = logging.FileLog.get_logger(args.debug, args.quiet_mode, __name__)
    logger.info(f'Invocation arguments: {args}')

    gx_cnx = db_conn.get_gx_cnx()
    sfi_cnx = db_conn.get_sfi_cnx()

    bf_cnx = db_conn.get_bankingfile_cnx()
    otdb_cnx = db_conn.get_otdb_cnx()


    #######################
    # date_T1  = date.today()
    ### override for PyCharm testing - from CLI there are arguments
    date_T1 = datetime.strptime(date_T1, '%Y-%m-%d').date() # last day of Q3
    ###
    #######################
    date_T = date_T1  + timedelta(days=1)
    if args.startdate:
        date_T1  = datetime.strptime(args.startdate, '%Y-%m-%d').date()
        date_T = date_T1
        if args.stopdate:
            date_T = datetime.strptime(args.stopdate, '%Y-%m-%d').date() + timedelta(days=1)
    logger.info(f"date_T1  {date_T1 } to date_T {date_T} (exclusive)")



    for single_date in daterange(date_T1 , date_T):
        # only process weekdays
        if single_date.weekday() < 5:
            logger.info(f"running process_date() for {single_date} (weekday={single_date.weekday()})")
            aacb_hc.get_hc_data(bf_cnx,sfi_cnx, single_date)
            aacb_hc.get_fx_rates(sfi_cnx)
            # Enrich the positions with some calculated data such as "lowest risk score" and market value in EUR
            # positions_df = get_positions(query_date, sfi_cnx, gx_cnx, bf_cnx, otdb_cnx)
            #
            # capital_requirement = capreq.process_capreq(positions_df)
            # logger.info(f"Capital Requirement for K-NPR and K-RTF met: {capital_requirement}")

            pos_lim_cal.pos_lim_forec(date_T, gx_cnx)
            # logger.info(f"K-DTF is: {kdtf}")

            # liquidty_requirement = liqreq.process_liqreq(positions_df)
            # logger.info(f"Liquidity Requirement met: {liquidty_requirement}")

        else:
            logger.debug(f"{single_date} is a weekend day (weekday={single_date.weekday()})")


if __name__ == '__main__':
    main()