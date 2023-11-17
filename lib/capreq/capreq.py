from lib.log import file_log as logging
from lib.utils.utils import printdf


def process_capreq(trades_df):

    logger = logging.FileLog.get_logger(name='_capreq_')
    logger.info("Running capital requirement classifications... reference if_class2_ind Tab I 18.00")

    for idx, row in trades_df.iterrows():

        if row['currency'] not in ('EUR', 'USD'):
            logger.info(f"{row['symbol']} ({row['isin']}) is {row['currency']} denominated issued by {row['cntry_terrtry_of_rsk']} ")


    # ToDo classification, calculation, display results in suitable manner for input to if_class2_ind Tab I 18.00

    return True