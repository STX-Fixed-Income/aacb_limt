import datetime
import logging
import os
from logging.handlers import TimedRotatingFileHandler

APP_NAME = 'dnb_corep'


class FileLog:
    @classmethod
    def get_logger(cls, debug_mode=False, quiet_mode=False, name=__name__):
        filename = f"{datetime.datetime.today().strftime('%Y%m%d')}_{APP_NAME}.log"
        logfile = "logs/" + filename
        if debug_mode:
            loglevel = logging.DEBUG
        else:
            loglevel = logging.INFO
        logging.getLogger().setLevel(loglevel)
        logger = logging.getLogger(name)
        logger.propagate = False
        if not logger.hasHandlers():
            # Add handlers to the logger
            if not quiet_mode or os.environ.get("AWS_EXECUTION_ENV") != None:
                # Create handlers
                c_handler = logging.StreamHandler()
                c_handler.setLevel(loglevel)
                c_format = logging.Formatter('%(asctime)s | %(levelname)s | %(name)s | %(message)s')
                c_handler.setFormatter(c_format)
                logger.addHandler(c_handler)
            # is this running in a AWS Lambda env? then only log to stdout
            if os.environ.get("AWS_EXECUTION_ENV") == None:
                f_handler = TimedRotatingFileHandler(filename=f"{logfile}", when='midnight', interval=1, backupCount=7)
                f_handler.rotation_filename = rotation_filename
                f_handler.setLevel(loglevel)
                f_format = logging.Formatter('%(asctime)s | %(levelname)s | %(name)s | %(message)s')
                f_handler.setFormatter(f_format)
                logger.addHandler(f_handler)
            elif os.environ.get("AWS_EXECUTION_ENV"):
                print(f'AWS_EXECUTION_ENV = {os.environ.get("AWS_EXECUTION_ENV")}')
        return logger


def rotation_filename(self):
    filename = f"{datetime.datetime.today().strftime('%Y%m%d')}_{APP_NAME}.log"
    logfile = "logs/" + filename
    return logfile

