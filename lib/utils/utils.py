import argparse
import boto3
import os
import sys
import subprocess
import time
from datetime import timedelta
import pandas as pd


def parse_arg():
    parser = argparse.ArgumentParser(description="dnb_corep display capital and liquidity requirement in real-time or"
                                                 "for a given query_date",
                                     formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("-d", "--debug", action='count', default=0, help="enable debugging")
    parser.add_argument("-q", "--quiet_mode", action="store_true", help="-q to disable logging console")
    parser.add_argument("--startdate", help="the first date to process")
    parser.add_argument("--stopdate", help="the last date to process")
    args = parser.parse_args()
    return args


def get_parameter(name: str):
    ssm = boto3.client('ssm', region_name='eu-central-1')
    parameter = ssm.get_parameter(Name=name, WithDecryption=True)
    if 'Parameter' in parameter and 'Value' in parameter['Parameter']:
        return parameter['Parameter']['Value']


def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)


def printdf(df):
    with pd.option_context('display.max_rows', None, 'display.max_columns', None,
                           'display.float_format', '{:.2f}'.format, 'display.expand_frame_repr', False):
        print(df)



def mount_drive(mnt_point, file, logger):
    admin_user = get_parameter('windows_admin_username')
    admin_pwd = get_parameter('windows_admin_password')
    mnt_str = f'sudo mount -t cifs -o vers=3.0,user={admin_user},password={admin_pwd} //sfi31fs01.stxfi.org/9Street/ {mnt_point}'
    if not os.path.exists(os.path.dirname(file)):
        logger.info("Mounting remote filesystem.")
        mount_src = subprocess.Popen(mnt_str, shell=True, stdout=subprocess.PIPE)
    time.sleep(2)
    if os.path.exists(os.path.dirname(file)):
        logger.info("Remote filesystem is mounted.")
    else:
        logger.error(f"File system not mounted: {mount_src}. Couldn't find: {file}")
        #umount_drive(mnt_point, logger)
        sys.exit(1)


def umount_drive(mnt_point, logger):
    mnt_str = f'sudo umount {mnt_point}'
    mount_src = subprocess.Popen(mnt_str, shell=True)
    logger.info(f"Attempting to un-Mounted remote filesystem. {mount_src}")
