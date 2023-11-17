from lib.log import file_log as logging
from lib.utils.utils import printdf

def process_liqreq(df):

    logger = logging.FileLog.get_logger(name='_liqreq_')
    logger.info("Running liquidity requirement classifications... reference if_class2_ind Tab I 09.00")

    for idx, row in df.iterrows():

        if row['iss_ind'] == 'GOVT NATIONAL':
            logger.info(f"{row['symbol']} ({row['isin']}) is a Govie issued by {row['cntry_terrtry_of_rsk']} "
                        f"with S&P rating {row['s&p_rating']} and Fitch rating {row['fitch']}. Is it Class 1?")


    printdf(df.head())
    print(df.entity_type.unique())
    return True
